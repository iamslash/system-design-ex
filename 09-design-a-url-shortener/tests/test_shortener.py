"""Tests for URL shortener components."""

from __future__ import annotations

import os
import sys

import fakeredis.aioredis
import pytest

# Add the api directory to the path so we can import modules.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "api"))

from shortener.base62 import encode, decode
from shortener.hash_approach import (
    generate_short_code,
    generate_with_collision_resolution,
    SHORT_CODE_LENGTH,
)
from shortener.id_generator import next_id, COUNTER_KEY, START_VALUE
from storage.redis_store import RedisURLStore


# ---------------------------------------------------------------------------
# Base62 Encode / Decode
# ---------------------------------------------------------------------------


class TestBase62:
    """Tests for Base62 encode/decode."""

    def test_encode_zero(self) -> None:
        """0 should encode to '0'."""
        assert encode(0) == "0"

    def test_encode_single_digit(self) -> None:
        """Values 0-61 should produce single characters."""
        assert encode(9) == "9"
        assert encode(10) == "a"
        assert encode(35) == "z"
        assert encode(36) == "A"
        assert encode(61) == "Z"

    def test_encode_multi_digit(self) -> None:
        """62 should encode to '10', 3843 to 'ZZ'."""
        assert encode(62) == "10"
        assert encode(3843) == "ZZ"

    def test_decode_single(self) -> None:
        """Decode single characters."""
        assert decode("0") == 0
        assert decode("Z") == 61

    def test_roundtrip(self) -> None:
        """Encoding then decoding should return the original value."""
        test_values = [0, 1, 61, 62, 100, 999, 100_000_000, 3_521_614_606_208]
        for val in test_values:
            assert decode(encode(val)) == val, f"Roundtrip failed for {val}"

    def test_encode_negative_raises(self) -> None:
        """Negative values should raise ValueError."""
        with pytest.raises(ValueError):
            encode(-1)

    def test_decode_invalid_char_raises(self) -> None:
        """Invalid characters should raise ValueError."""
        with pytest.raises(ValueError):
            decode("abc!")


# ---------------------------------------------------------------------------
# Hash Approach
# ---------------------------------------------------------------------------


class TestHashApproach:
    """Tests for hash-based short code generation."""

    def test_generate_short_code_length(self) -> None:
        """Generated code should be exactly SHORT_CODE_LENGTH characters."""
        code = generate_short_code("https://www.example.com")
        assert len(code) == SHORT_CODE_LENGTH

    def test_generate_deterministic(self) -> None:
        """Same URL should produce the same code."""
        url = "https://www.example.com/path"
        assert generate_short_code(url) == generate_short_code(url)

    def test_generate_different_urls(self) -> None:
        """Different URLs should (very likely) produce different codes."""
        code1 = generate_short_code("https://example.com/a")
        code2 = generate_short_code("https://example.com/b")
        assert code1 != code2

    def test_collision_resolution_no_collision(self) -> None:
        """Without collision, the first code should be returned."""
        code = generate_with_collision_resolution(
            "https://www.example.com",
            exists_fn=lambda c: False,  # No collision
        )
        assert len(code) == SHORT_CODE_LENGTH

    def test_collision_resolution_with_collisions(self) -> None:
        """When collisions occur, retries should produce a different code."""
        first_code = generate_short_code("https://www.example.com")
        call_count = 0

        def exists_fn(code: str) -> bool:
            nonlocal call_count
            call_count += 1
            # First call collides, second does not
            return call_count <= 1

        result = generate_with_collision_resolution(
            "https://www.example.com",
            exists_fn=exists_fn,
        )
        assert len(result) == SHORT_CODE_LENGTH
        assert result != first_code

    def test_collision_resolution_exhausted(self) -> None:
        """Should raise RuntimeError if all retries are exhausted."""
        with pytest.raises(RuntimeError, match="collision"):
            generate_with_collision_resolution(
                "https://www.example.com",
                exists_fn=lambda c: True,  # Always collides
            )

    def test_md5_mode(self) -> None:
        """MD5 mode should also produce valid short codes."""
        code = generate_short_code("https://www.example.com", use_md5=True)
        assert len(code) == SHORT_CODE_LENGTH


# ---------------------------------------------------------------------------
# ID Generator (Redis INCR)
# ---------------------------------------------------------------------------


class TestIdGenerator:
    """Tests for counter-based ID generator."""

    @pytest.mark.asyncio
    async def test_first_id(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """First ID should be START_VALUE + 1."""
        uid = await next_id(redis_client)
        assert uid == START_VALUE + 1

    @pytest.mark.asyncio
    async def test_sequential_ids(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """IDs should be sequential."""
        id1 = await next_id(redis_client)
        id2 = await next_id(redis_client)
        id3 = await next_id(redis_client)
        assert id2 == id1 + 1
        assert id3 == id2 + 1

    @pytest.mark.asyncio
    async def test_ids_are_unique(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """All generated IDs should be unique."""
        ids = set()
        for _ in range(100):
            uid = await next_id(redis_client)
            ids.add(uid)
        assert len(ids) == 100


# ---------------------------------------------------------------------------
# Redis URL Store
# ---------------------------------------------------------------------------


class TestRedisURLStore:
    """Tests for Redis-based URL storage."""

    @pytest.mark.asyncio
    async def test_save_and_get(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Saving a URL and retrieving by code should work."""
        store = RedisURLStore(redis_client)
        entry = await store.save("abc1234", "https://www.example.com")

        assert entry.short_code == "abc1234"
        assert entry.long_url == "https://www.example.com"
        assert entry.clicks == 0

        retrieved = await store.get_by_code("abc1234")
        assert retrieved is not None
        assert retrieved.long_url == "https://www.example.com"

    @pytest.mark.asyncio
    async def test_get_nonexistent(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Getting a non-existent code should return None."""
        store = RedisURLStore(redis_client)
        result = await store.get_by_code("nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_deduplication(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Same long URL should be mapped to the same short code."""
        store = RedisURLStore(redis_client)
        url = "https://www.example.com/dedup-test"

        await store.save("code1", url)

        # Query by long URL should return the existing code
        existing = await store.get_code_by_long_url(url)
        assert existing == "code1"

    @pytest.mark.asyncio
    async def test_click_counting(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Click count should increment on each call."""
        store = RedisURLStore(redis_client)
        await store.save("click1", "https://www.example.com")

        entry = await store.get_by_code("click1")
        assert entry.clicks == 0

        await store.increment_clicks("click1")
        await store.increment_clicks("click1")
        await store.increment_clicks("click1")

        entry = await store.get_by_code("click1")
        assert entry.clicks == 3

    @pytest.mark.asyncio
    async def test_code_exists(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """code_exists should return True for saved codes."""
        store = RedisURLStore(redis_client)
        assert not await store.code_exists("new_code")

        await store.save("new_code", "https://www.example.com")
        assert await store.code_exists("new_code")


# ---------------------------------------------------------------------------
# Integration: Shorten + Redirect flow
# ---------------------------------------------------------------------------


class TestShortenFlow:
    """Integration tests for the full shorten + retrieve flow."""

    @pytest.mark.asyncio
    async def test_base62_shorten_and_retrieve(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Base62 approach: shorten a URL and retrieve it."""
        store = RedisURLStore(redis_client)

        # Generate ID and encode
        from shortener.base62 import encode as b62_encode
        from shortener.id_generator import next_id as gen_id

        uid = await gen_id(redis_client)
        short_code = b62_encode(uid)

        await store.save(short_code, "https://www.example.com/long/path")

        entry = await store.get_by_code(short_code)
        assert entry is not None
        assert entry.long_url == "https://www.example.com/long/path"

    @pytest.mark.asyncio
    async def test_dedup_returns_same_code(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Shortening the same URL twice should return the same code."""
        store = RedisURLStore(redis_client)
        url = "https://www.example.com/same"

        await store.save("first_code", url)

        # Check dedup
        existing = await store.get_code_by_long_url(url)
        assert existing == "first_code"

    @pytest.mark.asyncio
    async def test_stats_after_clicks(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Stats should reflect click count after redirects."""
        store = RedisURLStore(redis_client)
        await store.save("stat1", "https://www.example.com")

        # Simulate 5 redirects
        for _ in range(5):
            await store.increment_clicks("stat1")

        entry = await store.get_by_code("stat1")
        assert entry.clicks == 5

    @pytest.mark.asyncio
    async def test_nonexistent_code_returns_none(self, redis_client: fakeredis.aioredis.FakeRedis) -> None:
        """Looking up a non-existent short code should return None."""
        store = RedisURLStore(redis_client)
        entry = await store.get_by_code("XXXXXXX")
        assert entry is None
