"""Email search using a simple inverted index.

Each word in the subject and body is tokenized and mapped to a set of
email_ids. Searching for a keyword returns all email_ids whose subject
or body contained that word.

Production systems would use Elasticsearch, but this demonstrates the
core inverted-index concept.
"""

from __future__ import annotations

import re

from redis import Redis

from models import Email


def _index_key(user: str, token: str) -> str:
    """Redis key for a single inverted-index entry."""
    return f"email:index:{user}:{token.lower()}"


def _tokenize(text: str) -> set[str]:
    """Split text into lowercase alphanumeric tokens."""
    return {t for t in re.split(r"\W+", text.lower()) if t}


def index_email(r: Redis, email: Email, user: str) -> None:
    """Add an email's subject and body tokens to the inverted index."""
    tokens = _tokenize(email.subject) | _tokenize(email.body)
    pipe = r.pipeline()
    for token in tokens:
        pipe.sadd(_index_key(user, token), email.email_id)
    pipe.execute()


def search_emails(r: Redis, user: str, query: str) -> list[str]:
    """Search for emails matching ALL query tokens (AND semantics).

    Returns a list of email_ids.
    """
    tokens = _tokenize(query)
    if not tokens:
        return []

    keys = [_index_key(user, t) for t in tokens]

    if len(keys) == 1:
        result = r.smembers(keys[0])
    else:
        result = r.sinter(*keys)

    return [eid if isinstance(eid, str) else eid.decode() for eid in result]
