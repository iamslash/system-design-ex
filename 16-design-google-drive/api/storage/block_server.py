"""Block server for file chunking, compression, and deduplication.

파일을 고정 크기 블록으로 분할하고, 각 블록을 SHA-256 해시하여
중복 블록은 한 번만 저장한다. zlib 압축으로 저장 공간을 절약한다.
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
    """파일 데이터를 고정 크기 블록으로 분할한다.

    Args:
        data: 원본 파일 바이트 데이터
        block_size: 블록 크기 (기본값: settings.BLOCK_SIZE)

    Returns:
        블록 리스트 (마지막 블록은 block_size 보다 작을 수 있다)
    """
    if block_size is None:
        block_size = settings.BLOCK_SIZE
    blocks: list[bytes] = []
    for i in range(0, len(data), block_size):
        blocks.append(data[i : i + block_size])
    return blocks


def compute_block_hash(block: bytes) -> str:
    """블록의 SHA-256 해시를 계산한다.

    동일한 내용의 블록은 항상 동일한 해시를 반환하므로
    중복 검사(dedup)에 사용된다.
    """
    return hashlib.sha256(block).hexdigest()


def compress_block(block: bytes) -> bytes:
    """블록을 zlib 으로 압축한다."""
    return zlib.compress(block)


def decompress_block(compressed: bytes) -> bytes:
    """압축된 블록을 원본으로 복원한다."""
    return zlib.decompress(compressed)


async def store_block(
    redis: Redis,
    block: bytes,
    storage_path: str | None = None,
) -> tuple[str, bool]:
    """블록을 저장한다. 이미 존재하는 블록이면 건너뛴다 (dedup).

    Args:
        redis: Redis 클라이언트 (블록 존재 여부 추적)
        block: 원본 블록 데이터
        storage_path: 블록 파일 저장 경로

    Returns:
        (block_hash, is_new) — 해시값과 새로 저장되었는지 여부
    """
    if storage_path is None:
        storage_path = settings.BLOCK_STORAGE_PATH

    block_hash = compute_block_hash(block)

    # 중복 검사: Redis 에 해시가 이미 있으면 저장하지 않는다
    exists = await redis.exists(f"block:{block_hash}")
    if exists:
        return block_hash, False

    # 압축 후 파일시스템에 저장
    compressed = compress_block(block)
    block_path = os.path.join(storage_path, block_hash)
    os.makedirs(storage_path, exist_ok=True)
    with open(block_path, "wb") as f:
        f.write(compressed)

    # Redis 에 블록 메타데이터 기록
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
    """저장된 블록을 읽어 압축 해제 후 반환한다.

    Args:
        block_hash: 블록의 SHA-256 해시
        storage_path: 블록 파일 저장 경로

    Returns:
        원본 블록 데이터

    Raises:
        FileNotFoundError: 블록 파일이 없을 때
    """
    if storage_path is None:
        storage_path = settings.BLOCK_STORAGE_PATH

    block_path = os.path.join(storage_path, block_hash)
    with open(block_path, "rb") as f:
        compressed = f.read()

    return decompress_block(compressed)
