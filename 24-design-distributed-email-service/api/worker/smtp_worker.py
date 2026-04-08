"""Simulated SMTP outgoing worker.

In production this would connect to a real SMTP server. Here it pops
emails from the Redis outgoing queue and calls deliver_to_inbox() to
simulate successful delivery.
"""

from __future__ import annotations

import json
import logging
import time

from redis import Redis

from models import Email
from email_service.receiver import deliver_to_inbox
from email_service.search import index_email

logger = logging.getLogger(__name__)

OUTGOING_QUEUE = "email:outgoing_queue"
DELIVERY_LOG = "email:delivery_log"


def process_one(r: Redis) -> bool:
    """Pop one email from the outgoing queue and deliver it.

    Returns True if a message was processed, False if queue was empty.
    """
    raw = r.lpop(OUTGOING_QUEUE)
    if raw is None:
        return False

    data = raw if isinstance(raw, str) else raw.decode()
    email = Email.model_validate_json(data)

    logger.info(
        "Delivering email %s from %s to %s",
        email.email_id,
        email.from_addr,
        email.to_addrs,
    )

    # Simulate SMTP delivery: store in recipients' inboxes
    delivered = deliver_to_inbox(r, email)

    # Index for sender search as well
    index_email(r, email, email.from_addr)

    # Log delivery
    log_entry = json.dumps(
        {
            "email_id": email.email_id,
            "from": email.from_addr,
            "to": email.to_addrs,
            "delivered_ids": delivered,
            "status": "delivered",
            "timestamp": email.created_at,
        }
    )
    r.rpush(DELIVERY_LOG, log_entry)

    return True


def run_worker(r: Redis, poll_interval: float = 1.0, max_iterations: int = 0) -> None:
    """Run the SMTP worker loop.

    Args:
        r: Redis connection.
        poll_interval: Seconds between queue polls.
        max_iterations: Stop after N iterations (0 = infinite).
    """
    logger.info("SMTP worker started, polling every %.1fs", poll_interval)
    iteration = 0
    while True:
        processed = process_one(r)
        if not processed:
            time.sleep(poll_interval)
        iteration += 1
        if max_iterations and iteration >= max_iterations:
            break
