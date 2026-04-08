"""WebSocket connection manager.

Tracks active WebSocket connections per user and provides methods for
sending messages to specific users or broadcasting to a group.
"""

from __future__ import annotations

import json
from typing import Any

from fastapi import WebSocket


class ConnectionManager:
    """활성 WebSocket 연결을 사용자별로 관리한다.

    하나의 사용자가 여러 디바이스에서 접속할 수 있으므로,
    user_id -> list[WebSocket] 형태로 다중 연결을 지원한다.
    """

    def __init__(self) -> None:
        # user_id -> list of active WebSocket connections
        self._connections: dict[str, list[WebSocket]] = {}

    async def connect(self, user_id: str, websocket: WebSocket) -> None:
        """새 WebSocket 연결을 수락하고 사용자 연결 목록에 추가한다."""
        await websocket.accept()
        if user_id not in self._connections:
            self._connections[user_id] = []
        self._connections[user_id].append(websocket)

    def disconnect(self, user_id: str, websocket: WebSocket) -> None:
        """사용자의 WebSocket 연결을 제거한다.

        해당 사용자의 마지막 연결이면 사용자 항목 자체를 삭제한다.
        """
        if user_id in self._connections:
            self._connections[user_id] = [
                ws for ws in self._connections[user_id] if ws is not websocket
            ]
            if not self._connections[user_id]:
                del self._connections[user_id]

    async def send_to_user(self, user_id: str, message: dict[str, Any]) -> None:
        """특정 사용자의 모든 연결에 메시지를 전송한다.

        사용자가 오프라인이면 메시지는 전송되지 않는다 (Redis 에 저장은 별도 처리).
        """
        if user_id in self._connections:
            payload = json.dumps(message)
            dead: list[WebSocket] = []
            for ws in self._connections[user_id]:
                try:
                    await ws.send_text(payload)
                except Exception:
                    dead.append(ws)
            # 끊어진 연결 정리
            for ws in dead:
                self.disconnect(user_id, ws)

    async def broadcast(self, user_ids: list[str], message: dict[str, Any]) -> None:
        """여러 사용자에게 메시지를 브로드캐스트한다."""
        for user_id in user_ids:
            await self.send_to_user(user_id, message)

    def is_connected(self, user_id: str) -> bool:
        """사용자가 현재 WebSocket 으로 접속 중인지 확인한다."""
        return user_id in self._connections and len(self._connections[user_id]) > 0

    def get_connected_users(self) -> list[str]:
        """현재 접속 중인 모든 사용자 ID 목록을 반환한다."""
        return list(self._connections.keys())


# 싱글톤 인스턴스
manager = ConnectionManager()
