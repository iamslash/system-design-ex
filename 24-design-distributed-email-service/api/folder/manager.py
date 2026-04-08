"""Folder management for email accounts.

Each user has default folders (Inbox, Sent, Drafts, Trash) and can
create custom folders. Emails can be moved between folders.
"""

from __future__ import annotations

import json

from redis import Redis

from models import Email, FolderType


DEFAULT_FOLDERS: list[FolderType] = [
    FolderType.INBOX,
    FolderType.SENT,
    FolderType.DRAFTS,
    FolderType.TRASH,
]


def _folder_key(user: str, folder: str) -> str:
    return f"email:folder:{user}:{folder}"


def _email_key(email_id: str) -> str:
    return f"email:msg:{email_id}"


def _custom_folders_key(user: str) -> str:
    return f"email:custom_folders:{user}"


def ensure_default_folders(r: Redis, user: str) -> list[str]:
    """Create default folder keys for a user. Returns folder names."""
    folders = [f.value for f in DEFAULT_FOLDERS]
    # Touch each folder set so it exists
    for folder in folders:
        r.sadd(_folder_key(user, folder), "__placeholder__")
        r.srem(_folder_key(user, folder), "__placeholder__")
    return folders


def create_custom_folder(r: Redis, user: str, folder_name: str) -> bool:
    """Create a custom folder. Returns True if created, False if exists."""
    added = r.sadd(_custom_folders_key(user), folder_name)
    return added > 0


def list_folders(r: Redis, user: str) -> list[str]:
    """List all folders for a user (default + custom)."""
    folders = [f.value for f in DEFAULT_FOLDERS]
    custom = r.smembers(_custom_folders_key(user))
    for name in custom:
        decoded = name if isinstance(name, str) else name.decode()
        folders.append(decoded)
    return folders


def get_folder_emails(r: Redis, user: str, folder: str) -> list[Email]:
    """Get all emails in a folder."""
    email_ids = r.smembers(_folder_key(user, folder))
    emails: list[Email] = []
    for eid in email_ids:
        decoded_id = eid if isinstance(eid, str) else eid.decode()
        data = r.get(_email_key(decoded_id))
        if data:
            emails.append(Email.model_validate_json(data))
    # Sort by created_at descending (newest first)
    emails.sort(key=lambda e: e.created_at, reverse=True)
    return emails


def move_email(
    r: Redis,
    user: str,
    email_id: str,
    from_folder: str,
    to_folder: str,
) -> bool:
    """Move an email from one folder to another.

    Returns True if the email was found and moved.
    """
    # Verify email exists in source folder
    if not r.sismember(_folder_key(user, from_folder), email_id):
        return False

    pipe = r.pipeline()
    pipe.srem(_folder_key(user, from_folder), email_id)
    pipe.sadd(_folder_key(user, to_folder), email_id)
    pipe.execute()

    # Update the email's folder field
    data = r.get(_email_key(email_id))
    if data:
        email_data = json.loads(data)
        email_data["folder"] = to_folder
        r.set(_email_key(email_id), json.dumps(email_data))

    return True


def delete_email(r: Redis, user: str, email_id: str, current_folder: str) -> bool:
    """Delete an email: move to Trash, or permanently delete if already in Trash."""
    if current_folder == FolderType.TRASH.value:
        # Permanent delete
        pipe = r.pipeline()
        pipe.srem(_folder_key(user, FolderType.TRASH.value), email_id)
        pipe.delete(_email_key(email_id))
        pipe.execute()
        return True
    else:
        return move_email(r, user, email_id, current_folder, FolderType.TRASH.value)


def get_folder_unread_count(r: Redis, user: str, folder: str) -> int:
    """Count unread emails in a folder."""
    email_ids = r.smembers(_folder_key(user, folder))
    count = 0
    for eid in email_ids:
        decoded_id = eid if isinstance(eid, str) else eid.decode()
        data = r.get(_email_key(decoded_id))
        if data:
            email_data = json.loads(data)
            if not email_data.get("is_read", False):
                count += 1
    return count
