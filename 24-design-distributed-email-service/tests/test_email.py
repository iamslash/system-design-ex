"""Tests for distributed email service.

Uses fakeredis so no running Redis instance is required.
Run: cd 24-design-distributed-email-service && pytest tests/ -v
"""

from __future__ import annotations

import fakeredis
import pytest

from models import Attachment, Email, FolderType
from email_service.sender import get_attachment, get_email, get_thread, send_email
from email_service.receiver import deliver_to_inbox, mark_as_read, mark_as_unread
from email_service.search import index_email, search_emails
from folder.manager import (
    create_custom_folder,
    delete_email,
    ensure_default_folders,
    get_folder_emails,
    get_folder_unread_count,
    list_folders,
    move_email,
)
from worker.smtp_worker import process_one


# ── Send ─────────────────────────────────────────────────────────────


class TestSendEmail:
    def test_send_basic(self, r: fakeredis.FakeRedis) -> None:
        """Send a basic email and verify it is stored."""
        email = send_email(
            r,
            from_addr="alice@example.com",
            to_addrs=["bob@example.com"],
            subject="Hello",
            body="Hi Bob!",
        )
        assert email.email_id
        assert email.from_addr == "alice@example.com"
        assert email.to_addrs == ["bob@example.com"]
        assert email.subject == "Hello"
        assert email.folder == FolderType.SENT

    def test_send_creates_thread(self, r: fakeredis.FakeRedis) -> None:
        """Sending an email should auto-assign a thread_id."""
        email = send_email(
            r,
            from_addr="alice@example.com",
            to_addrs=["bob@example.com"],
            subject="Thread test",
            body="First message",
        )
        assert email.thread_id is not None

    def test_send_stored_in_sent_folder(self, r: fakeredis.FakeRedis) -> None:
        """Sent email appears in sender's Sent folder."""
        email = send_email(
            r,
            from_addr="alice@example.com",
            to_addrs=["bob@example.com"],
            subject="Sent check",
            body="Body",
        )
        sent_ids = r.smembers("email:folder:alice@example.com:sent")
        assert email.email_id in sent_ids

    def test_send_queues_for_delivery(self, r: fakeredis.FakeRedis) -> None:
        """Sent email is pushed to the outgoing queue."""
        send_email(
            r,
            from_addr="alice@example.com",
            to_addrs=["bob@example.com"],
            subject="Queue test",
            body="Body",
        )
        queue_len = r.llen("email:outgoing_queue")
        assert queue_len == 1

    def test_send_with_cc(self, r: fakeredis.FakeRedis) -> None:
        """Send with CC recipients."""
        email = send_email(
            r,
            from_addr="alice@example.com",
            to_addrs=["bob@example.com"],
            cc_addrs=["carol@example.com"],
            subject="CC test",
            body="Body",
        )
        assert email.cc_addrs == ["carol@example.com"]

    def test_send_with_bcc(self, r: fakeredis.FakeRedis) -> None:
        """Send with BCC recipients."""
        email = send_email(
            r,
            from_addr="alice@example.com",
            to_addrs=["bob@example.com"],
            bcc_addrs=["dave@example.com"],
            subject="BCC test",
            body="Body",
        )
        assert email.bcc_addrs == ["dave@example.com"]


# ── Receive / Deliver ────────────────────────────────────────────────


class TestReceiveEmail:
    def test_deliver_to_inbox(self, r: fakeredis.FakeRedis) -> None:
        """Worker delivery places email in recipient inbox."""
        email = send_email(
            r,
            from_addr="alice@example.com",
            to_addrs=["bob@example.com"],
            subject="Deliver test",
            body="Hello",
        )
        deliver_to_inbox(r, email)
        inbox_ids = r.smembers("email:folder:bob@example.com:inbox")
        assert email.email_id in inbox_ids

    def test_deliver_marks_unread(self, r: fakeredis.FakeRedis) -> None:
        """Delivered email should be unread for recipient."""
        email = send_email(
            r,
            from_addr="alice@example.com",
            to_addrs=["bob@example.com"],
            subject="Unread test",
            body="Hello",
        )
        deliver_to_inbox(r, email)
        fetched = get_email(r, email.email_id)
        assert fetched is not None
        assert fetched.is_read is False

    def test_deliver_to_multiple_recipients(self, r: fakeredis.FakeRedis) -> None:
        """Email delivered to all TO and CC recipients."""
        email = send_email(
            r,
            from_addr="alice@example.com",
            to_addrs=["bob@example.com"],
            cc_addrs=["carol@example.com"],
            subject="Multi test",
            body="Hello all",
        )
        delivered = deliver_to_inbox(r, email)
        assert len(delivered) == 2
        assert email.email_id in r.smembers("email:folder:bob@example.com:inbox")
        assert email.email_id in r.smembers("email:folder:carol@example.com:inbox")

    def test_process_one_delivers(self, r: fakeredis.FakeRedis) -> None:
        """SMTP worker process_one pulls from queue and delivers."""
        send_email(
            r,
            from_addr="alice@example.com",
            to_addrs=["bob@example.com"],
            subject="Worker test",
            body="Hello",
        )
        assert process_one(r) is True
        inbox_ids = r.smembers("email:folder:bob@example.com:inbox")
        assert len(inbox_ids) == 1

    def test_process_one_empty_queue(self, r: fakeredis.FakeRedis) -> None:
        """process_one returns False when queue is empty."""
        assert process_one(r) is False


# ── Read / Unread ────────────────────────────────────────────────────


class TestReadUnread:
    def test_mark_as_read(self, r: fakeredis.FakeRedis) -> None:
        """Mark email as read."""
        email = send_email(
            r,
            from_addr="alice@example.com",
            to_addrs=["bob@example.com"],
            subject="Read test",
            body="Body",
        )
        deliver_to_inbox(r, email)
        assert mark_as_read(r, email.email_id) is True
        fetched = get_email(r, email.email_id)
        assert fetched is not None
        assert fetched.is_read is True

    def test_mark_as_unread(self, r: fakeredis.FakeRedis) -> None:
        """Mark email as unread after reading."""
        email = send_email(
            r,
            from_addr="alice@example.com",
            to_addrs=["bob@example.com"],
            subject="Unread test",
            body="Body",
        )
        mark_as_read(r, email.email_id)
        assert mark_as_unread(r, email.email_id) is True
        fetched = get_email(r, email.email_id)
        assert fetched is not None
        assert fetched.is_read is False

    def test_mark_nonexistent_email(self, r: fakeredis.FakeRedis) -> None:
        """Marking a nonexistent email returns False."""
        assert mark_as_read(r, "nonexistent") is False
        assert mark_as_unread(r, "nonexistent") is False


# ── Folder Operations ────────────────────────────────────────────────


class TestFolders:
    def test_default_folders(self, r: fakeredis.FakeRedis) -> None:
        """Users have default folders."""
        folders = ensure_default_folders(r, "alice@example.com")
        assert "inbox" in folders
        assert "sent" in folders
        assert "drafts" in folders
        assert "trash" in folders

    def test_list_folders(self, r: fakeredis.FakeRedis) -> None:
        """List all folders including custom."""
        ensure_default_folders(r, "alice@example.com")
        folders = list_folders(r, "alice@example.com")
        assert len(folders) >= 4

    def test_create_custom_folder(self, r: fakeredis.FakeRedis) -> None:
        """Create a custom folder."""
        assert create_custom_folder(r, "alice@example.com", "work") is True
        folders = list_folders(r, "alice@example.com")
        assert "work" in folders

    def test_create_duplicate_folder(self, r: fakeredis.FakeRedis) -> None:
        """Creating the same custom folder twice returns False."""
        create_custom_folder(r, "alice@example.com", "work")
        assert create_custom_folder(r, "alice@example.com", "work") is False

    def test_move_email_between_folders(self, r: fakeredis.FakeRedis) -> None:
        """Move email from inbox to trash."""
        email = send_email(
            r,
            from_addr="alice@example.com",
            to_addrs=["bob@example.com"],
            subject="Move test",
            body="Body",
        )
        deliver_to_inbox(r, email)

        assert move_email(
            r, "bob@example.com", email.email_id, "inbox", "trash"
        ) is True

        inbox_ids = r.smembers("email:folder:bob@example.com:inbox")
        trash_ids = r.smembers("email:folder:bob@example.com:trash")
        assert email.email_id not in inbox_ids
        assert email.email_id in trash_ids

    def test_move_nonexistent_email(self, r: fakeredis.FakeRedis) -> None:
        """Moving a nonexistent email returns False."""
        assert move_email(
            r, "bob@example.com", "fake_id", "inbox", "trash"
        ) is False

    def test_delete_moves_to_trash(self, r: fakeredis.FakeRedis) -> None:
        """First delete moves email to Trash."""
        email = send_email(
            r,
            from_addr="alice@example.com",
            to_addrs=["bob@example.com"],
            subject="Delete test",
            body="Body",
        )
        deliver_to_inbox(r, email)

        assert delete_email(r, "bob@example.com", email.email_id, "inbox") is True
        trash_ids = r.smembers("email:folder:bob@example.com:trash")
        assert email.email_id in trash_ids

    def test_delete_from_trash_permanent(self, r: fakeredis.FakeRedis) -> None:
        """Deleting from Trash is permanent."""
        email = send_email(
            r,
            from_addr="alice@example.com",
            to_addrs=["bob@example.com"],
            subject="Perm delete",
            body="Body",
        )
        deliver_to_inbox(r, email)
        # Move to trash first
        move_email(r, "bob@example.com", email.email_id, "inbox", "trash")
        # Delete from trash
        assert delete_email(r, "bob@example.com", email.email_id, "trash") is True
        # Email should be gone
        assert get_email(r, email.email_id) is None

    def test_folder_unread_count(self, r: fakeredis.FakeRedis) -> None:
        """Count unread emails in a folder."""
        e1 = send_email(
            r,
            from_addr="alice@example.com",
            to_addrs=["bob@example.com"],
            subject="Unread 1",
            body="Body",
        )
        e2 = send_email(
            r,
            from_addr="carol@example.com",
            to_addrs=["bob@example.com"],
            subject="Unread 2",
            body="Body",
        )
        deliver_to_inbox(r, e1)
        deliver_to_inbox(r, e2)

        count = get_folder_unread_count(r, "bob@example.com", "inbox")
        assert count == 2

        # Mark one as read
        mark_as_read(r, e1.email_id)
        count = get_folder_unread_count(r, "bob@example.com", "inbox")
        assert count == 1

    def test_get_folder_emails_sorted(self, r: fakeredis.FakeRedis) -> None:
        """Emails in a folder are sorted newest first."""
        e1 = send_email(
            r,
            from_addr="alice@example.com",
            to_addrs=["bob@example.com"],
            subject="First",
            body="Body",
        )
        e2 = send_email(
            r,
            from_addr="carol@example.com",
            to_addrs=["bob@example.com"],
            subject="Second",
            body="Body",
        )
        deliver_to_inbox(r, e1)
        deliver_to_inbox(r, e2)

        emails = get_folder_emails(r, "bob@example.com", "inbox")
        assert len(emails) == 2
        # Newest first
        assert emails[0].created_at >= emails[1].created_at


# ── Search ───────────────────────────────────────────────────────────


class TestSearch:
    def test_search_by_subject(self, r: fakeredis.FakeRedis) -> None:
        """Search finds email by subject keyword."""
        email = send_email(
            r,
            from_addr="alice@example.com",
            to_addrs=["bob@example.com"],
            subject="Quarterly Report",
            body="Please review the numbers.",
        )
        index_email(r, email, "bob@example.com")

        results = search_emails(r, "bob@example.com", "quarterly")
        assert email.email_id in results

    def test_search_by_body(self, r: fakeredis.FakeRedis) -> None:
        """Search finds email by body keyword."""
        email = send_email(
            r,
            from_addr="alice@example.com",
            to_addrs=["bob@example.com"],
            subject="Hello",
            body="The budget proposal is attached.",
        )
        index_email(r, email, "bob@example.com")

        results = search_emails(r, "bob@example.com", "budget")
        assert email.email_id in results

    def test_search_multi_word_and(self, r: fakeredis.FakeRedis) -> None:
        """Multi-word search uses AND semantics."""
        email = send_email(
            r,
            from_addr="alice@example.com",
            to_addrs=["bob@example.com"],
            subject="Project Alpha Update",
            body="Milestone reached.",
        )
        index_email(r, email, "bob@example.com")

        # Both words present
        results = search_emails(r, "bob@example.com", "project alpha")
        assert email.email_id in results

        # One word not present
        results = search_emails(r, "bob@example.com", "project beta")
        assert email.email_id not in results

    def test_search_no_results(self, r: fakeredis.FakeRedis) -> None:
        """Search with no matching keyword returns empty."""
        results = search_emails(r, "bob@example.com", "nonexistent")
        assert results == []

    def test_search_empty_query(self, r: fakeredis.FakeRedis) -> None:
        """Empty query returns empty."""
        results = search_emails(r, "bob@example.com", "")
        assert results == []

    def test_search_case_insensitive(self, r: fakeredis.FakeRedis) -> None:
        """Search is case-insensitive."""
        email = send_email(
            r,
            from_addr="alice@example.com",
            to_addrs=["bob@example.com"],
            subject="URGENT Notification",
            body="Action required.",
        )
        index_email(r, email, "bob@example.com")

        results = search_emails(r, "bob@example.com", "urgent")
        assert email.email_id in results


# ── Threading ────────────────────────────────────────────────────────


class TestThreading:
    def test_reply_continues_thread(self, r: fakeredis.FakeRedis) -> None:
        """Replying to an email continues the same thread."""
        original = send_email(
            r,
            from_addr="alice@example.com",
            to_addrs=["bob@example.com"],
            subject="Original",
            body="First message",
        )
        reply = send_email(
            r,
            from_addr="bob@example.com",
            to_addrs=["alice@example.com"],
            subject="Re: Original",
            body="Reply message",
            in_reply_to=original.email_id,
        )
        assert reply.thread_id == original.thread_id
        assert reply.in_reply_to == original.email_id

    def test_get_thread_ordered(self, r: fakeredis.FakeRedis) -> None:
        """get_thread returns emails in chronological order."""
        e1 = send_email(
            r,
            from_addr="alice@example.com",
            to_addrs=["bob@example.com"],
            subject="Start",
            body="First",
        )
        e2 = send_email(
            r,
            from_addr="bob@example.com",
            to_addrs=["alice@example.com"],
            subject="Re: Start",
            body="Second",
            in_reply_to=e1.email_id,
        )
        thread = get_thread(r, e1.thread_id)
        assert len(thread) == 2
        assert thread[0].email_id == e1.email_id
        assert thread[1].email_id == e2.email_id

    def test_new_email_new_thread(self, r: fakeredis.FakeRedis) -> None:
        """Emails without in_reply_to get distinct thread_ids."""
        e1 = send_email(
            r,
            from_addr="alice@example.com",
            to_addrs=["bob@example.com"],
            subject="Thread A",
            body="Body",
        )
        e2 = send_email(
            r,
            from_addr="alice@example.com",
            to_addrs=["bob@example.com"],
            subject="Thread B",
            body="Body",
        )
        assert e1.thread_id != e2.thread_id


# ── Attachment Handling ──────────────────────────────────────────────


class TestAttachments:
    def test_send_with_attachment(self, r: fakeredis.FakeRedis) -> None:
        """Send email with attachment metadata."""
        att = Attachment(
            filename="report.pdf",
            content_type="application/pdf",
            size=1024,
        )
        email = send_email(
            r,
            from_addr="alice@example.com",
            to_addrs=["bob@example.com"],
            subject="Report",
            body="See attached.",
            attachments=[att],
        )
        assert len(email.attachments) == 1
        assert email.attachments[0].filename == "report.pdf"

    def test_get_attachment(self, r: fakeredis.FakeRedis) -> None:
        """Retrieve attachment metadata by ID."""
        att = Attachment(
            filename="image.png",
            content_type="image/png",
            size=2048,
        )
        email = send_email(
            r,
            from_addr="alice@example.com",
            to_addrs=["bob@example.com"],
            subject="Photo",
            body="Here is the photo.",
            attachments=[att],
        )
        fetched = get_attachment(r, email.email_id, email.attachments[0].attachment_id)
        assert fetched is not None
        assert fetched.filename == "image.png"
        assert fetched.size == 2048

    def test_get_nonexistent_attachment(self, r: fakeredis.FakeRedis) -> None:
        """Getting a nonexistent attachment returns None."""
        assert get_attachment(r, "fake", "fake") is None

    def test_multiple_attachments(self, r: fakeredis.FakeRedis) -> None:
        """Send email with multiple attachments."""
        atts = [
            Attachment(filename="a.txt", content_type="text/plain", size=100),
            Attachment(filename="b.jpg", content_type="image/jpeg", size=5000),
        ]
        email = send_email(
            r,
            from_addr="alice@example.com",
            to_addrs=["bob@example.com"],
            subject="Files",
            body="Two files.",
            attachments=atts,
        )
        assert len(email.attachments) == 2
        for att in email.attachments:
            fetched = get_attachment(r, email.email_id, att.attachment_id)
            assert fetched is not None


# ── Integration: Full Send-Deliver-Read Cycle ────────────────────────


class TestIntegration:
    def test_full_cycle(self, r: fakeredis.FakeRedis) -> None:
        """Full cycle: send -> worker deliver -> recipient reads."""
        # Alice sends to Bob
        email = send_email(
            r,
            from_addr="alice@example.com",
            to_addrs=["bob@example.com"],
            subject="Integration test",
            body="Full cycle test body",
        )

        # Worker delivers
        assert process_one(r) is True

        # Bob's inbox has the email
        inbox = get_folder_emails(r, "bob@example.com", "inbox")
        assert len(inbox) == 1
        assert inbox[0].email_id == email.email_id
        assert inbox[0].is_read is False

        # Bob reads the email
        mark_as_read(r, email.email_id)
        fetched = get_email(r, email.email_id)
        assert fetched is not None
        assert fetched.is_read is True

        # Bob searches for the email
        results = search_emails(r, "bob@example.com", "integration")
        assert email.email_id in results

    def test_full_thread_cycle(self, r: fakeredis.FakeRedis) -> None:
        """Full thread: send -> deliver -> reply -> deliver -> check thread."""
        # Alice sends to Bob
        e1 = send_email(
            r,
            from_addr="alice@example.com",
            to_addrs=["bob@example.com"],
            subject="Discussion",
            body="Let us discuss the plan.",
        )
        process_one(r)

        # Bob replies
        e2 = send_email(
            r,
            from_addr="bob@example.com",
            to_addrs=["alice@example.com"],
            subject="Re: Discussion",
            body="Sounds good, let us meet tomorrow.",
            in_reply_to=e1.email_id,
        )
        process_one(r)

        # Thread has both emails
        thread = get_thread(r, e1.thread_id)
        assert len(thread) >= 2
        ids = [e.email_id for e in thread]
        assert e1.email_id in ids
        assert e2.email_id in ids
