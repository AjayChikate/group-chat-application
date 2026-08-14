"""
rooms.py
------------------------------------------------------------
Tracks chat rooms and who is currently in each one.

The baseline tutorial had exactly one implicit "room" (the
whole server). Real chat apps isolate broadcast scope by
room/channel, so a message sent in #tech never reaches
someone sitting in #random. RoomManager is the component
responsible for that isolation: it owns the room -> members
mapping and is the only thing allowed to broadcast into a room.
------------------------------------------------------------
"""
import json
import time


def _safe_send(member: dict, payload: str) -> None:
    """Send raw JSON payload to one member, thread-safe, silent on failure
    (a dead socket is cleaned up by its own connection thread, not here)."""
    ws = member.get('ws')
    if not ws or not getattr(ws, 'connected', False):
        return
    lock = member.get('lock')
    try:
        if lock:
            with lock:
                ws.send(payload)
        else:
            ws.send(payload)
    except Exception:
        pass


class RoomManager:
    def __init__(self, logger):
        self.logger = logger
        # room_name -> {'name': str, 'members': {client_id: member_dict}, 'created_at': float}
        self.rooms = {}

    def ensure_room(self, name: str) -> dict:
        if name not in self.rooms:
            self.rooms[name] = {'name': name, 'members': {}, 'created_at': time.time()}
            self.logger.log('room_created', room=name)
        return self.rooms[name]

    def room_exists(self, name: str) -> bool:
        return name in self.rooms

    def list_rooms(self) -> list:
        rooms = [
            {'name': r['name'], 'memberCount': len(r['members'])}
            for r in self.rooms.values()
        ]
        return sorted(rooms, key=lambda r: r['name'])

    def join(self, room_name: str, client_id: str, member: dict) -> None:
        room = self.ensure_room(room_name)
        room['members'][client_id] = member

    def leave(self, room_name: str, client_id: str) -> None:
        room = self.rooms.get(room_name)
        if room:
            room['members'].pop(client_id, None)

    def get_members(self, room_name: str) -> list:
        room = self.rooms.get(room_name)
        return list(room['members'].values()) if room else []

    def get_usernames(self, room_name: str) -> list:
        return [m['username'] for m in self.get_members(room_name)]

    def broadcast(self, room_name: str, obj: dict, exclude_client_id: str = None) -> None:
        """Send `obj` to every socket in `room_name`, optionally skipping one client."""
        room = self.rooms.get(room_name)
        if not room:
            return
        payload = json.dumps(obj)
        for cid, member in list(room['members'].items()):
            if cid == exclude_client_id:
                continue
            _safe_send(member, payload)