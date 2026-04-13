"""Block server for file chunking, compression, and deduplication.

Splits files into fixed-size blocks, SHA-256 hashes each block, and
stores duplicate blocks only once. Uses zlib compression to save storage space.
"""

from __future__ import annotations

import hashlib
import os
import zlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from redis.asyncio import Redis

from config import settings


def split_into_blocks(data: bytes, block_size: int | None = None) -> list[bytes]:
    """Split file data into fixed-size blocks.

    Args:
        data: Raw file bytes
        block_size: Block size in bytes (default: settings.BLOCK_SIZE)

    Returns:
        List of blocks; the last block may be smaller than block_size
    """
    if block_size is None:
        block_size = settings.BLOCK_SIZE
    blocks: list[bytes] = []
    for i in range(0, len(data), block_size):
        blocks.append(data[i : i + block_size])
    return blocks


def compute_block_hash(block: bytes) -> str:
    """Compute the SHA-256 hash of a block.

    Blocks with identical content always return the same hash,
    so this is used for deduplication checks.
    """
    return hashlib.sha256(block).hexdigest()


def compress_block(block: bytes) -> bytes:
    """Compress a block using zlib."""
    return zlib.compress(block)


def decompress_block(compressed: bytes) -> bytes:
    """Decompress a compressed block back to its original form."""
    return zlib.decompress(compressed)


async def store_block(
    redis: Redis,
    block: bytes,
    storage_path: str | None = None,
) -> tuple[str, bool]:
    """Store a block, skipping it if it already exists (dedup).

    Args:
        redis: Redis client (tracks block existence)
        block: Raw block data
        storage_path: Directory path for block file storage

    Returns:
        (block_hash, is_new) — the hash and whether the block was newly stored
    """
    if storage_path is None:
        storage_path = settings.BLOCK_STORAGE_PATH

    block_hash = compute_block_hash(block)

    # Dedup check: skip storing if the hash already exists in Redis
    exists = await redis.exists(f"block:{block_hash}")
    if exists:
        return block_hash, False

    # Compress and write to the filesystem
    compressed = compress_block(block)
    block_path = os.path.join(storage_path, block_hash)
    os.makedirs(storage_path, exist_ok=True)
    with open(block_path, "wb") as f:
        f.write(compressed)

    # Record block metadata in Redis
    await redis.hset(
        f"block:{block_hash}",
        mapping={
            "original_size": str(len(block)),
            "compressed_size": str(len(compressed)),
        },
    )

    return block_hash, True


async def load_block(
    block_hash: str,
    storage_path: str | None = None,
) -> bytes:
    """Read a stored block, decompress it, and return the original data.

    Args:
        block_hash: SHA-256 hash of the block
        storage_path: Directory path where block files are stored

    Returns:
        Original block data

    Raises:
        FileNotFoundError: When the block file does not exist
    """
    if storage_path is None:
        storage_path = settings.BLOCK_STORAGE_PATH

    block_path = os.path.join(storage_path, block_hash)
    with open(block_path, "rb") as f:
        compressed = f.read()

    return decompress_block(compressed)
