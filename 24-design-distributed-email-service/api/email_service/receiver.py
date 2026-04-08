"""Email receiving and storage.

When the SMTP worker "delivers" an email, receiver.deliver_to_inbox()
stores it in each recipient's Inbox folder and indexes it for search.
"""

from __future__ import annotations

import json

from redis import Redis

from models import Email, FolderType
from email_service.search import index_email


def _email_key(email_id: str) -> str:
    return f"email:msg:{email_id}"


def _folder_key(user: str, folder: str) -> str:
    return f"email:folder:{user}:{folder}"


def _thread_key(thread_id: str) -> str:
    return f"email:thread:{thread_id}"


def deliver_to_inbox(r: Redis, email: Email) -> list[str]:
    """Deliver an email to all recipients' Inbox folders.

    Creates a per-recipient copy with is_read=False and folder=INBOX.
    Returns a list of recipient-specific email_ids.
    """
    all_recipients = list(set(email.to_addrs + email.cc_addrs))
    delivered_ids: list[str] = []

    for recipient in all_recipients:
        # Create a recipient-local copy
        inbox_email = email.model_copy(
            update={
                "is_read": False,
                "folder": FolderType.INBOX,
            }
        )

        pipe = r.pipeline()

        # Store the email (keyed by same email_id so threading works)
        pipe.set(_email_key(inbox_email.email_id), inbox_email.model_dump_json())

        # Add to recipient's inbox
        pipe.sadd(
            _folder_key(recipient, FolderType.INBOX.value),
            inbox_email.email_id,
        )

        # Add to thread tracking
        if inbox_email.thread_id:
            # Only add if not already present (sender already added it)
            existing = r.lrange(_thread_key(inbox_email.thread_id), 0, -1)
            existing_decoded = [
                e if isinstance(e, str) else e.decode() for e in existing
            ]
            if inbox_email.email_id not in existing_decoded:
                pipe.rpush(_thread_key(inbox_email.thread_id), inbox_email.email_id)

        pipe.execute()

        # Index for search
        index_email(r, inbox_email, recipient)

        delivered_ids.append(inbox_email.email_id)

    return delivered_ids


def mark_as_read(r: Redis, email_id: str) -> bool:
    """Mark an email as read."""
    return _set_read_status(r, email_id, True)


def mark_as_unread(r: Redis, email_id: str) -> bool:
    """Mark an email as unread."""
    return _set_read_status(r, email_id, False)


def _set_read_status(r: Redis, email_id: str, is_read: bool) -> bool:
    """Set the read/unread status of an email."""
    key = _email_key(email_id)
    data = r.get(key)
    if data is None:
        return False
    email_data = json.loads(data)
    email_data["is_read"] = is_read
    r.set(key, json.dumps(email_data))
    return True
