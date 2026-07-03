"""In-memory per-session chat history — a rolling window, not persisted to
disk (process restart clears it, same as main.py's existing _chat_contexts).
"""

_MAX_MESSAGES = 24


class ChatMemory:
    def __init__(self):
        self._sessions: dict[str, list[dict]] = {}

    def get(self, session_id: str) -> list[dict]:
        return self._sessions.setdefault(session_id, [])

    def append(self, session_id: str, role: str, content: str):
        history = self.get(session_id)
        history.append({"role": role, "content": content})
        if len(history) > _MAX_MESSAGES:
            del history[:-_MAX_MESSAGES]

    def clear(self, session_id: str):
        self._sessions.pop(session_id, None)
