"""FastAPI application for distributed email service."""

from __future__ import annotations

import threading
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import redis
from fastapi import FastAPI, HTTPException

import config
from models import (
    Email,
    FolderCreateRequest,
    FolderType,
    MoveEmailRequest,
    SearchRequest,
    SendEmailRequest,
)
from email_service.sender import get_attachment, get_email, get_thread, send_email
from email_service.receiver import mark_as_read, mark_as_unread
from email_service.search import search_emails
from folder.manager import (
    create_custom_folder,
    delete_email,
    ensure_default_folders,
    get_folder_emails,
    get_folder_unread_count,
    list_folders,
    move_email,
)
from worker.smtp_worker import process_one, run_worker


def get_redis() -> redis.Redis:
    return redis.Redis(
        host=config.REDIS_HOST,
        port=config.REDIS_PORT,
        decode_responses=True,
    )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Start the SMTP worker thread on startup."""
    r = get_redis()
    worker_thread = threading.Thread(
        target=run_worker,
        args=(r,),
        kwargs={"poll_interval": config.WORKER_POLL_INTERVAL},
        daemon=True,
    )
    worker_thread.start()
    yield


app = FastAPI(
    title="Distributed Email Service",
    description="Simplified email service with queue-based sending, folder management, and search.",
    version="1.0.0",
    lifespan=lifespan,
)


# --- Send / Receive ---


@app.post("/api/email/send", response_model=Email)
def api_send_email(req: SendEmailRequest) -> Email:
    """Send an email. Queues for async SMTP delivery."""
    r = get_redis()
    email = send_email(
        r,
        from_addr=req.from_addr,
        to_addrs=req.to_addrs,
        cc_addrs=req.cc_addrs,
        bcc_addrs=req.bcc_addrs,
        subject=req.subject,
        body=req.body,
        attachments=req.attachments,
        in_reply_to=req.in_reply_to,
    )
    return email


@app.post("/api/email/deliver")
def api_deliver_now() -> dict:
    """Manually trigger delivery of queued emails (for testing)."""
    r = get_redis()
    count = 0
    while process_one(r):
        count += 1
    return {"delivered": count}


@app.get("/api/email/{email_id}", response_model=Email)
def api_get_email(email_id: str) -> Email:
    """Retrieve a single email by ID."""
    r = get_redis()
    email = get_email(r, email_id)
    if email is None:
        raise HTTPException(status_code=404, detail="Email not found")
    return email


@app.post("/api/email/{email_id}/read")
def api_mark_read(email_id: str) -> dict:
    """Mark an email as read."""
    r = get_redis()
    if not mark_as_read(r, email_id):
        raise HTTPException(status_code=404, detail="Email not found")
    return {"status": "ok", "email_id": email_id, "is_read": True}


@app.post("/api/email/{email_id}/unread")
def api_mark_unread(email_id: str) -> dict:
    """Mark an email as unread."""
    r = get_redis()
    if not mark_as_unread(r, email_id):
        raise HTTPException(status_code=404, detail="Email not found")
    return {"status": "ok", "email_id": email_id, "is_read": False}


@app.get("/api/email/{email_id}/attachment/{attachment_id}")
def api_get_attachment(email_id: str, attachment_id: str) -> dict:
    """Retrieve attachment metadata."""
    r = get_redis()
    att = get_attachment(r, email_id, attachment_id)
    if att is None:
        raise HTTPException(status_code=404, detail="Attachment not found")
    return att


# --- Threads ---


@app.get("/api/thread/{thread_id}", response_model=list[Email])
def api_get_thread(thread_id: str) -> list[Email]:
    """Get all emails in a thread."""
    r = get_redis()
    return get_thread(r, thread_id)


# --- Folders ---


@app.get("/api/folders/{user}")
def api_list_folders(user: str) -> dict:
    """List all folders for a user."""
    r = get_redis()
    ensure_default_folders(r, user)
    return {"user": user, "folders": list_folders(r, user)}


@app.get("/api/folders/{user}/{folder}")
def api_get_folder(user: str, folder: str) -> dict:
    """Get all emails in a folder."""
    r = get_redis()
    emails = get_folder_emails(r, user, folder)
    return {"user": user, "folder": folder, "count": len(emails), "emails": emails}


@app.get("/api/folders/{user}/{folder}/unread")
def api_folder_unread(user: str, folder: str) -> dict:
    """Get unread count for a folder."""
    r = get_redis()
    count = get_folder_unread_count(r, user, folder)
    return {"user": user, "folder": folder, "unread_count": count}


@app.post("/api/folders/create")
def api_create_folder(req: FolderCreateRequest) -> dict:
    """Create a custom folder."""
    r = get_redis()
    created = create_custom_folder(r, req.user, req.folder_name)
    if not created:
        raise HTTPException(status_code=409, detail="Folder already exists")
    return {"status": "created", "user": req.user, "folder": req.folder_name}


@app.post("/api/email/move")
def api_move_email(req: MoveEmailRequest) -> dict:
    """Move an email to a different folder."""
    r = get_redis()
    email = get_email(r, req.email_id)
    if email is None:
        raise HTTPException(status_code=404, detail="Email not found")
    current_folder = email.folder if isinstance(email.folder, str) else email.folder.value
    # Determine user context: prefer to_addrs for inbox mail, from_addr for sent
    user = email.to_addrs[0] if email.to_addrs else email.from_addr
    success = move_email(r, user, req.email_id, current_folder, req.target_folder.value)
    if not success:
        raise HTTPException(status_code=400, detail="Move failed")
    return {"status": "moved", "email_id": req.email_id, "folder": req.target_folder}


@app.delete("/api/email/{user}/{email_id}")
def api_delete_email(user: str, email_id: str) -> dict:
    """Delete an email (move to trash, or permanent if already in trash)."""
    r = get_redis()
    email = get_email(r, email_id)
    if email is None:
        raise HTTPException(status_code=404, detail="Email not found")
    current_folder = email.folder if isinstance(email.folder, str) else email.folder.value
    success = delete_email(r, user, email_id, current_folder)
    if not success:
        raise HTTPException(status_code=400, detail="Delete failed")
    return {"status": "deleted", "email_id": email_id}


# --- Search ---


@app.post("/api/email/search")
def api_search(req: SearchRequest) -> dict:
    """Search emails by keyword."""
    r = get_redis()
    email_ids = search_emails(r, req.user, req.query)
    emails = [get_email(r, eid) for eid in email_ids]
    emails = [e for e in emails if e is not None]
    return {"query": req.query, "user": req.user, "count": len(emails), "emails": emails}


# --- Health ---


@app.get("/health")
def health() -> dict:
    """Health check."""
    r = get_redis()
    try:
        r.ping()
        return {"status": "healthy", "redis": "connected"}
    except Exception:
        raise HTTPException(status_code=503, detail="Redis unavailable")
