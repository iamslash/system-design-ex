"""Append-only data store for object data.

Design inspired by real object storage systems:
- Objects are appended to large data files (similar to write-ahead logs).
- An object_mapping tracks (object_id -> file_name, offset, size) for retrieval.
- Files are rotated when they exceed MAX_FILE_SIZE.
- Compaction removes dead (deleted/overwritten) objects to reclaim space.
"""

from __future__ import annotations

import os
import threading
import uuid
from dataclasses import dataclass, field


@dataclass
class ObjectLocation:
    """Location of an object within the append-only data store."""

    file_name: str
    offset: int
    size: int


@dataclass
class DataStore:
    """Append-only file-based data store.

    Multiple objects are packed sequentially into data files.
    The object_mapping dict provides O(1) lookup by object_id.
    """

    data_dir: str
    max_file_size: int = 64 * 1024 * 1024  # 64 MB

    # Internal state
    _current_file: str = ""
    _current_offset: int = 0
    _object_mapping: dict[str, ObjectLocation] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def __post_init__(self) -> None:
        os.makedirs(self.data_dir, exist_ok=True)
        self._rotate_file()

    def _rotate_file(self) -> None:
        """Create a new data file for appending."""
        file_id = uuid.uuid4().hex[:12]
        self._current_file = os.path.join(self.data_dir, f"data_{file_id}.dat")
        self._current_offset = 0

    def put(self, data: bytes) -> str:
        """Append object data and return a unique object_id.

        Thread-safe: acquires a lock for the append operation.
        """
        with self._lock:
            # Rotate if the current file exceeds max size
            if self._current_offset + len(data) > self.max_file_size:
                self._rotate_file()

            object_id = uuid.uuid4().hex

            with open(self._current_file, "ab") as f:
                f.write(data)

            location = ObjectLocation(
                file_name=self._current_file,
                offset=self._current_offset,
                size=len(data),
            )
            self._object_mapping[object_id] = location
            self._current_offset += len(data)

            return object_id

    def get(self, object_id: str) -> bytes | None:
        """Read object data by object_id using the mapping table."""
        location = self._object_mapping.get(object_id)
        if location is None:
            return None

        with open(location.file_name, "rb") as f:
            f.seek(location.offset)
            return f.read(location.size)

    def delete(self, object_id: str) -> bool:
        """Mark an object as deleted by removing it from the mapping.

        The data remains on disk until compaction reclaims space.
        """
        with self._lock:
            if object_id in self._object_mapping:
                del self._object_mapping[object_id]
                return True
            return False

    def exists(self, object_id: str) -> bool:
        """Check if an object_id exists in the mapping."""
        return object_id in self._object_mapping

    def get_location(self, object_id: str) -> ObjectLocation | None:
        """Return the storage location for an object."""
        return self._object_mapping.get(object_id)

    def compact(self) -> int:
        """Compact all data files: rewrite only live objects.

        Returns the number of bytes reclaimed.
        """
        with self._lock:
            # Group live objects by their source file
            file_objects: dict[str, list[tuple[str, ObjectLocation]]] = {}
            for oid, loc in self._object_mapping.items():
                file_objects.setdefault(loc.file_name, []).append((oid, loc))

            reclaimed = 0

            # Process each file that has live objects
            for file_name, objects in file_objects.items():
                # Calculate total file size on disk
                if not os.path.exists(file_name):
                    continue
                original_size = os.path.getsize(file_name)
                live_size = sum(loc.size for _, loc in objects)

                # Skip if no dead data to reclaim
                if live_size >= original_size:
                    continue

                # Read live data and rewrite
                new_file = file_name + ".compact"
                new_offset = 0

                with open(new_file, "wb") as wf:
                    for oid, loc in sorted(objects, key=lambda x: x[1].offset):
                        with open(file_name, "rb") as rf:
                            rf.seek(loc.offset)
                            data = rf.read(loc.size)
                        wf.write(data)
                        self._object_mapping[oid] = ObjectLocation(
                            file_name=file_name,
                            offset=new_offset,
                            size=loc.size,
                        )
                        new_offset += loc.size

                # Replace original with compacted file
                os.replace(new_file, file_name)
                reclaimed += original_size - live_size

            return reclaimed

    @property
    def object_count(self) -> int:
        """Number of live objects in the store."""
        return len(self._object_mapping)

    @property
    def object_mapping(self) -> dict[str, ObjectLocation]:
        """Read-only access to the object mapping."""
        return dict(self._object_mapping)
