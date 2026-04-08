"""Queue-based email sending.

Emails are pushed onto a Redis list (outgoing queue). The SMTP worker
pulls from the queue and delivers them asynchronously.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

from redis import Redis

from models import Attachment, Email, FolderType

# Redis keys
OUTGOING_QUEUE = "email:outgoing_queue"


def _email_key(email_id: str) -> str:
    return f"email:msg:{email_id}"


def _attachment_key(email_id: str, attachment_id: str) -> str:
    return f"email:attachment:{email_id}:{attachment_id}"


def _folder_key(user: str, folder: str) -> str:
    return f"email:folder:{user}:{folder}"


def _thread_key(thread_id: str) -> str:
    return f"email:thread:{thread_id}"


def send_email(
    r: Redis,
    *,
    from_addr: str,
    to_addrs: list[str],
    cc_addrs: list[str] | None = None,
    bcc_addrs: list[str] | None = None,
    subject: str = "",
    body: str = "",
    attachments: list[Attachment] | None = None,
    in_reply_to: str | None = None,
) -> Email:
    """Enqueue an email for sending and store it in the sender's Sent folder.

    Returns the created Email object.
    """
    cc_addrs = cc_addrs or []
    bcc_addrs = bcc_addrs or []
    attachments = attachments or []

    # Determine thread_id: continue existing thread or start new one
    thread_id: str | None = None
    if in_reply_to:
        parent_data = r.get(_email_key(in_reply_to))
        if parent_data:
            parent = json.loads(parent_data)
            thread_id = parent.get("thread_id") or in_reply_to
    if not thread_id:
        thread_id = uuid.uuid4().hex

    email = Email(
        from_addr=from_addr,
        to_addrs=to_addrs,
        cc_addrs=cc_addrs,
        bcc_addrs=bcc_addrs,
        subject=subject,
        body=body,
        attachments=attachments,
        thread_id=thread_id,
        in_reply_to=in_reply_to,
        is_read=True,  # Sender has "read" their own email
        folder=FolderType.SENT,
        created_at=datetime.now(timezone.utc).isoformat(),
    )

    pipe = r.pipeline()

    # Store email metadata
    pipe.set(_email_key(email.email_id), email.model_dump_json())

    # Store attachment refs separately
    for att in attachments:
        pipe.set(
            _attachment_key(email.email_id, att.attachment_id),
            att.model_dump_json(),
        )

    # Add to sender's Sent folder
    pipe.sadd(_folder_key(from_addr, FolderType.SENT.value), email.email_id)

    # Add to thread
    pipe.rpush(_thread_key(thread_id), email.email_id)

    # Enqueue for SMTP worker delivery
    pipe.rpush(OUTGOING_QUEUE, email.model_dump_json())

    pipe.execute()
    return email


def get_email(r: Redis, email_id: str) -> Email | None:
    """Retrieve a single email by ID."""
    data = r.get(_email_key(email_id))
    if data is None:
        return None
    return Email.model_validate_json(data)


def get_thread(r: Redis, thread_id: str) -> list[Email]:
    """Retrieve all emails in a thread, ordered chronologically."""
    email_ids = r.lrange(_thread_key(thread_id), 0, -1)
    emails: list[Email] = []
    for eid in email_ids:
        email = get_email(r, eid if isinstance(eid, str) else eid.decode())
        if email:
            emails.append(email)
    return emails


def get_attachment(r: Redis, email_id: str, attachment_id: str) -> Attachment | None:
    """Retrieve attachment metadata."""
    data = r.get(_attachment_key(email_id, attachment_id))
    if data is None:
        return None
    return Attachment.model_validate_json(data)
