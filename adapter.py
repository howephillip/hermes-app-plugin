"""Hermes App platform adapter: a live, two-way connection between the Hermes app and the gateway.

One WebSocket per phone. Every message the agent sends (replies, follow-ups, cron deliveries) is first
written to an append-only log (HERMES_HOME/hermes-app/log.jsonl) and then pushed to connected phones,
so a phone that was offline asks for "everything after seq N" when it reconnects.

Python standard library only. Config (env, set by the app's setup command):
  HERMES_APP_ENABLED=true turns the platform on. The app proves who it is with the Hermes API key it already
  holds (API_SERVER_KEY, the same key it uses to chat), so there is no second key to keep in sync.
  HERMES_APP_TOKEN is still accepted too, for installs made by older versions.
  HERMES_APP_PORT (8648), HERMES_APP_HOST (0.0.0.0), HERMES_APP_HOME_CHANNEL (home),
  HERMES_APP_ALLOW_ALL_USERS (true: the key is the credential).
"""

import asyncio
import base64
import contextlib
import datetime
import hashlib
import hmac
import json
import logging
import mimetypes
import os
import pathlib
import re
import shlex
import shutil
import struct
import subprocess
import sys
import time
import unicodedata
import urllib.parse
import uuid
from collections import OrderedDict
from typing import Any, Dict, Optional

from gateway.config import Platform
from gateway.platforms._shared import get_scoped_secret as _get_scoped_secret
from gateway.platforms._shared import seed_extra_from_env as _seed_extra_from_env
from gateway.platforms.base import BasePlatformAdapter, SendResult
from gateway.platforms.event import MessageEvent, MessageType

logger = logging.getLogger(__name__)

PLATFORM = "hermes_app"
PLUGIN_NAME = "hermes-app"
PLUGIN_VERSION = "1.5.4"  # keep in step with plugin.yaml; the app compares it to know when to offer an update
DEFAULT_PORT = 8648
PROTOCOL = 1
# What this plugin can do, announced to the app on connect so a newer app can tell what an older plugin lacks.
FEATURES = ["kinds", "activity", "update", "files", "agents", "streaming"]
LOG_KEEP = 5000  # entries kept in the log across restarts
MAX_FRAME = 24 * 1024 * 1024  # a photo or two, base64 encoded
MAX_REPLAY = 500
WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
HOME_CHAT = "home"


# ── paths and the log ────────────────────────────────────────────────────────

def _home() -> pathlib.Path:
    return pathlib.Path(os.environ.get("HERMES_HOME") or (pathlib.Path.home() / ".hermes"))


def _data_dir() -> pathlib.Path:
    d = _home() / "hermes-app"
    d.mkdir(parents=True, exist_ok=True)
    return d


_last_seq = 0


def _next_seq() -> int:
    """Strictly increasing, and comparable across processes (the cron sender appends from outside)."""
    global _last_seq
    _last_seq = max(time.time_ns() // 1000, _last_seq + 1)
    return _last_seq


def _append(entry: dict) -> dict:
    """Write one entry to the log. A single small O_APPEND write is atomic, so no lock is needed."""
    entry = dict(entry, seq=_next_seq(), ts=entry.get("ts") or datetime.datetime.now(datetime.timezone.utc).isoformat())
    line = (json.dumps(entry, separators=(",", ":")) + "\n").encode()
    fd = os.open(_data_dir() / "log.jsonl", os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
    try:
        os.write(fd, line)
    finally:
        os.close(fd)
    return entry


def _read_log(after: int, limit: int = MAX_REPLAY) -> list:
    path = _data_dir() / "log.jsonl"
    if not path.exists():
        return []
    out = []
    with open(path, "rb") as fh:
        for raw in fh:
            try:
                e = json.loads(raw)
            except ValueError:
                continue
            if e.get("seq", 0) > after:
                out.append(e)
    return out[-limit:]


def _trim_log() -> None:
    path = _data_dir() / "log.jsonl"
    if not path.exists():
        return
    lines = path.read_bytes().splitlines()
    if len(lines) > LOG_KEEP:
        tmp = path.with_suffix(".tmp")
        tmp.write_bytes(b"\n".join(lines[-LOG_KEEP:]) + b"\n")
        os.replace(tmp, path)


def _latest_seq() -> int:
    entries = _read_log(0, limit=1)
    return entries[-1]["seq"] if entries else 0


# ── files the agent sends (documents, images, audio) ─────────────────────────

MAX_FILE = 25 * 1024 * 1024   # per file; the app keeps received files in the chat, so this bounds chat size
FILE_TTL = 7 * 24 * 3600      # a file nobody downloaded for a week is cleaned up
FILES_CAP = 1024 * 1024 * 1024  # total on disk; oldest go first
_FILE_ID = re.compile(r"^[0-9a-f]{32}$")
_AUDIO_EXT = {".mp3": "audio/mpeg", ".m4a": "audio/mp4", ".aac": "audio/aac", ".wav": "audio/wav",
              ".ogg": "audio/ogg", ".opus": "audio/ogg", ".flac": "audio/flac", ".aiff": "audio/aiff", ".caf": "audio/x-caf"}


class FileRejected(Exception):
    """A file that can't be delivered (missing, unsafe, too big). The message is shown to the user."""


def _files_dir() -> pathlib.Path:
    d = _data_dir() / "files"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _safe_name(name: str) -> str:
    base = os.path.basename(str(name or "")).strip()
    base = "".join(ch for ch in base if ch.isprintable() and ch not in '/\\:*?"<>|')[:120].strip(" .")
    return base or "file"


def _mime_for(name: str) -> str:
    ext = os.path.splitext(name)[1].lower()
    return _AUDIO_EXT.get(ext) or mimetypes.guess_type(name)[0] or "application/octet-stream"


def _kind_of_file(mime: str) -> str:
    for prefix, kind in (("image/", "image"), ("audio/", "audio"), ("video/", "video")):
        if mime.startswith(prefix):
            return kind
    return "file"


def _human_size(n: int) -> str:
    return f"{n / (1024 * 1024):.1f} MB" if n >= 1024 * 1024 else f"{max(1, n // 1024)} KB"


def _store_file(path: str, voice: bool = False, name: Optional[str] = None) -> dict:
    """Copy a file the agent produced into our own folder and describe it for the app. The copy means the agent
    deleting a temp file (text-to-speech output, say) can't break delivery. Raises ``FileRejected``."""
    safe = BasePlatformAdapter.validate_media_delivery_path(str(path))  # Hermes's own path safety, checked again
    if not safe:
        raise FileRejected("That file isn't available to send.")
    src = pathlib.Path(safe)
    try:
        size = src.stat().st_size
    except OSError:
        raise FileRejected("That file isn't available to send.")
    label = _safe_name(name or src.name)
    if size > MAX_FILE:
        raise FileRejected(f"{label} is too large to send to the app ({_human_size(size)}; the limit is {_human_size(MAX_FILE)}).")
    fid = uuid.uuid4().hex
    folder = _files_dir() / fid
    folder.mkdir(parents=True)
    try:
        shutil.copyfile(src, folder / "blob")
        mime = _mime_for(label)
        meta = {"name": label, "mime": mime, "size": size, "created": time.time()}
        (folder / "meta.json").write_text(json.dumps(meta))
    except OSError as exc:
        shutil.rmtree(folder, ignore_errors=True)
        raise FileRejected(f"Couldn't prepare {label}: {exc.strerror or exc}")
    return {"id": fid, "name": label, "mime": mime, "size": size, "kind": _kind_of_file(mime), "voice": bool(voice)}


def _open_file(fid: str):
    """(blob path, meta) for a stored file id, else None. Ids are random 128-bit hex, so there is no path to walk."""
    if not _FILE_ID.match(fid or ""):
        return None
    folder = _files_dir() / fid
    try:
        meta = json.loads((folder / "meta.json").read_text())
        blob = folder / "blob"
        return (blob, meta) if blob.is_file() else None
    except (OSError, ValueError):
        return None


def _cleanup_files() -> None:
    """Drop files older than a week, then the oldest until the folder is under the cap."""
    try:
        entries = []
        now = time.time()
        for folder in _files_dir().iterdir():
            blob = folder / "blob"
            if not blob.is_file():
                shutil.rmtree(folder, ignore_errors=True)
                continue
            age_ref = blob.stat().st_mtime
            if now - age_ref > FILE_TTL:
                shutil.rmtree(folder, ignore_errors=True)
                continue
            entries.append((age_ref, blob.stat().st_size, folder))
        total = sum(e[1] for e in entries)
        for _ts, size, folder in sorted(entries):
            if total <= FILES_CAP:
                break
            shutil.rmtree(folder, ignore_errors=True)
            total -= size
    except OSError:
        pass


def _downgrade(entry: dict) -> dict:
    """What an app that predates files sees: the text plus a plain note per attachment."""
    out = {k: v for k, v in entry.items() if k != "attachments"}
    notes = "\n".join(f"📎 {a.get('name', 'file')}" for a in entry.get("attachments") or [])
    out["text"] = ((entry.get("text") or "") + ("\n\n" if entry.get("text") else "") + notes
                   + "\n(Update the Hermes app to receive files.)").strip()
    return out


# ── minimal WebSocket (RFC 6455) ─────────────────────────────────────────────

class _Closed(Exception):
    pass


async def _read_frame(reader: asyncio.StreamReader):
    """Return (opcode, payload) of one complete message, joining fragments. Client frames are masked."""
    message = b""
    first_op = None
    while True:
        head = await reader.readexactly(2)
        # Plain arithmetic instead of bit masks: the setup command embeds this file and Hermes's terminal
        # tool refuses any command containing an ampersand.
        fin, op = head[0] // 128, head[0] % 16
        masked, length = head[1] // 128, head[1] % 128
        if length == 126:
            length = struct.unpack(">H", await reader.readexactly(2))[0]
        elif length == 127:
            length = struct.unpack(">Q", await reader.readexactly(8))[0]
        if length > MAX_FRAME or not masked:
            raise _Closed()
        mask = await reader.readexactly(4)
        data = bytearray(await reader.readexactly(length))
        for i in range(length):
            data[i] ^= mask[i % 4]
        if op >= 0x8:  # control frames are never fragmented
            return op, bytes(data)
        if first_op is None:
            first_op = op
        message += bytes(data)
        if len(message) > MAX_FRAME:
            raise _Closed()
        if fin:
            return first_op, message


def _frame(op: int, payload: bytes) -> bytes:
    n = len(payload)
    if n < 126:
        head = struct.pack(">BB", 0x80 | op, n)
    elif n < 65536:
        head = struct.pack(">BBH", 0x80 | op, 126, n)
    else:
        head = struct.pack(">BBQ", 0x80 | op, 127, n)
    return head + payload


class _Client:
    def __init__(self, writer: asyncio.StreamWriter):
        self.writer = writer
        self.authed = False
        self.device = ""
        self.features: list = []  # what this app says it can do (e.g. "files"), sent in its hello

    async def send_json(self, obj: dict) -> None:
        if obj.get("attachments") and "files" not in self.features:
            obj = _downgrade(obj)
        self.writer.write(_frame(0x1, json.dumps(obj, separators=(",", ":")).encode()))
        await self.writer.drain()


# ── what kind of message is this ─────────────────────────────────────────────

_PROGRESS_VERBS = frozenset((
    "Running", "Reading", "Writing", "Searching", "Fetching", "Editing", "Listing", "Browsing", "Using", "Calling",
    "Executing", "Patching", "Scanning", "Opening", "Creating", "Deleting", "Installing", "Checking", "Looking",
    "Analyzing", "Analysing", "Thinking", "Remembering", "Recalling", "Saving", "Updating", "Loading", "Querying",
    "Downloading", "Uploading", "Navigating", "Clicking", "Typing", "Taking", "Delegating", "Planning", "Testing",
    "Building", "Finding", "Grepping", "Globbing", "Viewing", "Copying", "Moving", "Fixing", "Generating",
    "Synthesizing", "Synthesising", "Transcribing", "Converting", "Rendering",
))


# "⚙️ tool_search: ..." : a tool's own name (snake_case with an underscore) followed by a colon
_TOOL_NAME_LABEL = re.compile(r"^[a-z][a-z0-9]*(_[a-z0-9]+)+:$")


def _looks_like_progress(text: str) -> bool:
    """Tool-progress bubbles Hermes builds ("💻 Running grep ...", "📖 Reading x.py"): every line is a symbol
    followed by a known tool verb. Only used for sends Hermes did not label either way, and kept strict so an
    ordinary reply that happens to start with an emoji is never taken for status."""
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    if not lines or len(text) > 2000:
        return False
    for ln in lines:
        if unicodedata.category(ln[0]) not in ("So", "Sk"):
            return False
        rest = ln[1:].lstrip("\ufe0f\u200d ").strip()
        word = rest.split(" ", 1)[0] if rest else ""
        if not word or (word not in _PROGRESS_VERBS and not _TOOL_NAME_LABEL.match(word)):
            return False
    return True


def _kind_for(content: str, metadata: Optional[dict]) -> str:
    """"reply" (what the agent says to the user, spoken aloud in voice mode) or "status" (memory notices, interim
    chatter, tool progress: shown quietly, never spoken).

    Hermes labels what matters: a finished reply carries ``notify`` and mid-turn status carries ``_interim_send``.
    Unlabelled sends are streamed answer chunks or tool-progress bubbles, told apart by their shape."""
    md = metadata or {}
    if md.get("_interim_send"):
        return "status"
    if md.get("notify"):
        return "reply"
    return "status" if _looks_like_progress(content) else "reply"


def _brief(args) -> str:
    """A short, readable hint of what a tool is doing, for the activity indicator."""
    if not isinstance(args, dict):
        return ""
    for key in ("command", "cmd", "path", "file_path", "filename", "url", "query", "pattern", "name"):
        val = args.get(key)
        if isinstance(val, str) and val.strip():
            return " ".join(val.split())[:80]
    return ""


# ── adapter ──────────────────────────────────────────────────────────────────

class HermesAppAdapter(BasePlatformAdapter):
    """Serves the app's WebSocket and bridges it to the gateway."""

    serial_receive_loop = False

    def __init__(self, config, **kwargs):
        super().__init__(config=config, platform=Platform(PLATFORM))
        extra = getattr(config, "extra", {}) or {}
        self.tokens = _accepted_keys(extra)
        self.host = str(_get_scoped_secret("HERMES_APP_HOST", "") or extra.get("host") or "0.0.0.0")
        self.port = _port(_get_scoped_secret("HERMES_APP_PORT", "") or extra.get("port"))
        self._server: Optional[asyncio.AbstractServer] = None
        self._clients: set = set()
        self._pump_task: Optional[asyncio.Task] = None
        self._wake: Optional[asyncio.Event] = None  # set when this process appends, so delivery needn't wait for a poll
        self._offset = 0
        self._updating = False
        self._active_chats: set = set()  # chats with a turn in flight, for attributing tool activity
        self._active_since: Dict[str, float] = {}  # chat -> when its turn began, so a turn that never reported back expires
        self._stream_noted: set = set()  # what the log already said about streaming, so it says it once
        self._kinds: "OrderedDict[str, str]" = OrderedDict()  # message id -> kind, so an edit keeps its message's kind
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._seen: "OrderedDict[str, None]" = OrderedDict()  # inbound ids already handled (client retries)
        # Replies being written right now, per chat: [{mid, text, sent, at, ended, timer}]. Hermes has gateway streaming
        # off for most people and won't let a plugin switch it on, so the plugin watches the model's words itself and
        # shows them as they come; Hermes's own final message is then folded into the same bubble (see _claim_stream).
        self._streams: Dict[str, list] = {}

    @property
    def name(self) -> str:
        return "Hermes App"

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        if not self.tokens:
            logger.error("Hermes App: no API key found. Set API_SERVER_KEY (the key the app chats with).")
            self._set_fatal_error("config_missing", "API_SERVER_KEY must be set", retryable=False)
            return False
        if not self._acquire_platform_lock(PLATFORM, f"{self.host}:{self.port}", f"Hermes App port {self.port}"):
            return False
        try:
            _trim_log()
            await asyncio.to_thread(_cleanup_files)
            self._offset = (_data_dir() / "log.jsonl").stat().st_size if (_data_dir() / "log.jsonl").exists() else 0
            self._server = await asyncio.start_server(self._on_connection, self.host, self.port)
        except OSError as exc:
            logger.error("Hermes App: cannot listen on %s:%s: %s", self.host, self.port, exc)
            self._set_fatal_error("listen_failed", str(exc), retryable=True)
            return False
        self._wake = asyncio.Event()
        self._loop = asyncio.get_running_loop()
        _ACTIVE["adapter"] = self
        self._pump_task = asyncio.create_task(self._pump())
        self._mark_connected()
        logger.info("Hermes App: listening on %s:%s", self.host, self.port)
        self._wire_plugin_handlers(None)
        return True

    async def disconnect(self) -> None:
        if _ACTIVE.get("adapter") is self:
            _ACTIVE["adapter"] = None
        with contextlib.suppress(Exception):
            self._release_platform_lock()
        self._mark_disconnected()
        if self._pump_task:
            self._pump_task.cancel()
            with contextlib.suppress(BaseException):
                await self._pump_task
            self._pump_task = None
        for client in list(self._clients):
            with contextlib.suppress(Exception):
                client.writer.write(_frame(0x8, b"\x03\xe9"))
                client.writer.close()
        self._clients.clear()
        if self._server:
            self._server.close()
            with contextlib.suppress(Exception):
                await self._server.wait_closed()
            self._server = None

    # -- outbound: gateway -> phone ------------------------------------------

    async def send(self, chat_id: str, content: str, reply_to: Optional[str] = None,
                   metadata: Optional[Dict[str, Any]] = None):
        kind = _kind_for(content, metadata)
        claimed = self._claim_stream(str(chat_id), content)
        if claimed:  # the words were already on the phone as they were written: finish that bubble, don't add another
            self._remember_kind(claimed, kind, metadata)
            self._record({"t": "edit", "id": claimed, "chat": str(chat_id), "text": content, "kind": kind})
            return SendResult(success=True, message_id=claimed)
        mid = uuid.uuid4().hex
        self._remember_kind(mid, kind, metadata)
        entry = self._record({"t": "msg", "id": mid, "chat": str(chat_id), "text": content, "kind": kind})
        return SendResult(success=True, message_id=entry["id"])

    # -- streaming: the model's words, as they are written --------------------

    STREAM_EVERY = 0.3  # seconds between updates to the phone (each is one log entry)

    def stream_event(self, event: str, payload: dict) -> None:
        """Called from Hermes's stream hooks on a worker thread."""
        loop = self._loop
        if loop is None or loop.is_closed():
            return
        loop.call_soon_threadsafe(self._stream_apply, event, payload)

    def _stream_chat(self) -> Optional[str]:
        # The hook says which platform spoke but not which chat; with exactly one turn in flight it is that one.
        # With several at once it can't be told, so nothing is streamed and the whole reply arrives as before.
        now = time.monotonic()
        for chat in [c for c in self._active_chats if now - self._active_since.get(c, now) > 900]:
            self._active_chats.discard(chat)  # a turn that never reported back must not block streaming for ever
            self._active_since.pop(chat, None)
        chats = list(self._active_chats)
        if len(chats) != 1:
            self._note_stream("ambiguous:%s" % ",".join(sorted(chats)),
                              "Hermes App: not streaming, %d chats in flight %s" % (len(chats), sorted(chats)))
            return None
        return chats[0]

    def _note_stream(self, key: str, message: str) -> None:
        if key not in self._stream_noted:
            if len(self._stream_noted) > 50:
                self._stream_noted.clear()
            self._stream_noted.add(key)
            logger.info(message)

    def _stream_apply(self, event: str, payload: dict) -> None:
        chat = self._stream_chat()
        if chat is None:
            return
        open_ = self._streams.setdefault(chat, [])
        if event == "on_stream_start":
            open_.append({"mid": None, "text": "", "sent": "", "at": 0.0, "ended": False, "timer": None})
            del open_[:-8]
            return
        if not open_ or open_[-1]["ended"]:
            if event == "on_stream_end":
                return
            open_.append({"mid": None, "text": "", "sent": "", "at": 0.0, "ended": False, "timer": None})
        state = open_[-1]
        if event == "on_stream_delta":
            if payload.get("kind", "text") != "text":
                return
            state["text"] += str(payload.get("delta") or "")
            self._stream_flush(chat, state, force=False)
        elif event == "on_stream_end":
            state["ended"] = True
            self._stream_flush(chat, state, force=True)

    def _stream_flush(self, chat: str, state: dict, force: bool) -> None:
        loop = self._loop
        now = time.monotonic()
        if not force and now - state["at"] < self.STREAM_EVERY:
            if state["timer"] is None and loop is not None:
                state["timer"] = loop.call_later(self.STREAM_EVERY, self._stream_flush_later, chat, state)
            return
        self._stream_cancel_timer(state)
        text = state["text"]
        if not text.strip() or text == state["sent"]:
            return
        state["at"], state["sent"] = now, text
        if state["mid"] is None:
            state["mid"] = uuid.uuid4().hex
            self._remember_kind(state["mid"], "reply", None)
            self._record({"t": "msg", "id": state["mid"], "chat": chat, "text": text, "kind": "reply"})
        else:
            self._record({"t": "edit", "id": state["mid"], "chat": chat, "text": text, "kind": "reply"})

    def _stream_flush_later(self, chat: str, state: dict) -> None:
        state["timer"] = None
        self._stream_flush(chat, state, force=True)

    @staticmethod
    def _stream_cancel_timer(state: dict) -> None:
        timer = state.get("timer")
        if timer is not None:
            timer.cancel()
        state["timer"] = None

    @staticmethod
    def _same_words(a: str, b: str) -> bool:
        a, b = " ".join(a.split()), " ".join(b.split())
        if not a or not b:
            return False
        key = min(40, len(a), len(b))
        if key >= 8 and a[:key] == b[:key]:
            return True
        short, long_ = (a, b) if len(a) <= len(b) else (b, a)
        return len(short) >= 20 and short in long_

    def _claim_stream(self, chat: str, content: str) -> Optional[str]:
        """The streamed bubble that is this same text, if there is one: it is then finished with Hermes's final wording
        instead of a second bubble being added."""
        open_ = self._streams.get(chat) or []
        for state in reversed(open_):
            if state["mid"] and self._same_words(state["text"], content):
                self._stream_cancel_timer(state)
                open_.remove(state)
                return state["mid"]
        return None

    def _finish_streams(self, chat: str) -> None:
        for state in self._streams.pop(chat, []):
            self._stream_flush(chat, state, force=True)
            self._stream_cancel_timer(state)

    async def _send_file(self, chat_id: str, path: str, caption: Optional[str], voice: bool = False,
                         name: Optional[str] = None):
        try:
            att = await asyncio.to_thread(_store_file, path, voice, name)
        except FileRejected as exc:
            # Tell the user why in the chat, and let Hermes know the delivery failed.
            self._record({"t": "msg", "id": uuid.uuid4().hex, "chat": str(chat_id), "text": f"📎 {exc}", "kind": "status"})
            return SendResult(success=False, error=str(exc))
        except Exception as exc:  # noqa: BLE001
            logger.warning("Hermes App: could not store %s: %s", path, exc)
            return SendResult(success=False, error=f"Could not prepare the file: {exc}")
        mid = uuid.uuid4().hex
        self._remember_kind(mid, "reply", None)
        entry = self._record({"t": "msg", "id": mid, "chat": str(chat_id), "text": caption or "", "kind": "reply",
                              "attachments": [att]})
        return SendResult(success=True, message_id=entry["id"])

    async def send_document(self, chat_id: str, file_path: str, caption: Optional[str] = None,
                            file_name: Optional[str] = None, reply_to: Optional[str] = None,
                            metadata: Optional[Dict[str, Any]] = None, **kwargs):
        return await self._send_file(chat_id, file_path, caption, name=file_name)

    async def send_image_file(self, chat_id: str, image_path: str, caption: Optional[str] = None,
                              reply_to: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None, **kwargs):
        return await self._send_file(chat_id, image_path, caption)

    async def send_voice(self, chat_id: str, audio_path: str, caption: Optional[str] = None,
                         reply_to: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None, **kwargs):
        return await self._send_file(chat_id, audio_path, caption, voice=True)

    async def send_video(self, chat_id: str, video_path: str, caption: Optional[str] = None,
                         reply_to: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None, **kwargs):
        return await self._send_file(chat_id, video_path, caption)

    async def send_or_update_status(self, chat_id: str, status_key: str, content: str, metadata=None):
        """Hermes routes the agent's own status lines here (memory recall, compaction, lifecycle notices): by
        definition not a reply, so they are labelled status and never spoken. Each one is its own line."""
        mid = uuid.uuid4().hex
        self._kinds[mid] = "status:explicit"
        entry = self._record({"t": "msg", "id": mid, "chat": str(chat_id), "text": content, "kind": "status"})
        return SendResult(success=True, message_id=entry["id"])

    async def edit_message(self, chat_id: str, message_id: str, content: str, *, finalize: bool = False):
        # Status Hermes labelled stays status; anything else is re-judged as its text grows (streamed replies, and
        # progress bubbles that are edited as more tools run).
        kind = "status" if self._kinds.get(message_id) == "status:explicit" else _kind_for(content, None)
        self._remember_kind(message_id, kind, None)
        self._record({"t": "edit", "id": message_id, "chat": str(chat_id), "text": content, "kind": kind})
        return SendResult(success=True, message_id=message_id)

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        await self._broadcast({"t": "typing", "chat": str(chat_id), "on": True})

    async def stop_typing(self, chat_id: str) -> None:
        await self._broadcast({"t": "typing", "chat": str(chat_id), "on": False})

    async def on_processing_complete(self, event, outcome) -> None:
        """The agent finished this turn (success or not): tell the phone so it stops showing 'working'."""
        self._finish_streams(str(event.source.chat_id))
        self._active_chats.discard(str(event.source.chat_id))
        self._active_since.pop(str(event.source.chat_id), None)
        # Through the log like messages, so "done" can never overtake the reply it closes.
        self._record({"t": "done", "chat": str(event.source.chat_id), "ok": getattr(outcome, "value", "") == "success"})

    async def get_chat_info(self, chat_id: str) -> dict:
        return {"name": "Hermes App", "type": "dm", "chat_id": chat_id}

    def _remember_kind(self, mid: str, kind: str, metadata: Optional[dict]) -> None:
        explicit = bool((metadata or {}).get("_interim_send"))
        self._kinds[mid] = "status:explicit" if (explicit and kind == "status") else kind
        while len(self._kinds) > 500:
            self._kinds.popitem(last=False)

    def emit_activity(self, tool: str, detail: str) -> None:
        """Tell the phone which tool the agent is using (a quiet "working" line, never a chat message). Safe to call
        from the agent's worker thread."""
        loop = self._loop
        if loop is None or loop.is_closed():
            return
        chats = list(self._active_chats) or [self._home_chat()]
        frames = [{"t": "activity", "chat": c, "tool": tool, "detail": detail} for c in chats]
        for frame in frames:
            loop.call_soon_threadsafe(lambda f=frame: asyncio.ensure_future(self._broadcast(f)))

    def _record(self, entry: dict) -> dict:
        entry = _append(entry)
        if self._wake is not None:
            self._wake.set()
        return entry

    async def _broadcast(self, obj: dict) -> None:
        for client in list(self._clients):
            if not client.authed:
                continue
            try:
                await client.send_json(obj)
            except Exception:
                self._clients.discard(client)

    async def _pump(self) -> None:
        """Push log entries to connected phones. Everything (replies, edits, cron deliveries from other
        processes) goes through the log, so this is the one delivery path."""
        path = _data_dir() / "log.jsonl"
        while True:
            try:  # woken at once by this process's own appends; polls for ones from other processes (cron)
                await asyncio.wait_for(self._wake.wait(), timeout=0.25)
            except asyncio.TimeoutError:
                pass
            self._wake.clear()
            try:
                size = path.stat().st_size
            except OSError:
                continue
            if size < self._offset:  # log was trimmed or replaced
                self._offset = 0
            if size == self._offset:
                continue
            with open(path, "rb") as fh:
                fh.seek(self._offset)
                chunk = fh.read(size - self._offset)
            # Only consume whole lines; a partial last line is picked up next tick.
            end = chunk.rfind(b"\n") + 1
            self._offset += end
            for raw in chunk[:end].splitlines():
                try:
                    entry = json.loads(raw)
                except ValueError:
                    continue
                await self._broadcast(entry)

    # -- inbound: phone -> gateway -------------------------------------------

    async def _on_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        client = _Client(writer)
        try:
            if not await self._handshake(reader, writer):
                return
            self._clients.add(client)
            while True:
                # A client that connects must say hello promptly; after that it may stay idle.
                op, data = await (_read_frame(reader) if client.authed
                                  else asyncio.wait_for(_read_frame(reader), timeout=10))
                if op == 0x8:
                    break
                if op == 0x9:
                    writer.write(_frame(0xA, data))
                    continue
                if op != 0x1:
                    continue
                try:
                    msg = json.loads(data)
                except ValueError:
                    continue
                if not client.authed:
                    if not await self._hello(client, msg):
                        break
                    continue
                await self._handle_frame(client, msg)
        except (asyncio.IncompleteReadError, asyncio.TimeoutError, ConnectionError, _Closed, asyncio.CancelledError):
            pass
        except Exception:
            logger.exception("Hermes App: connection error")
        finally:
            self._clients.discard(client)
            with contextlib.suppress(Exception):
                writer.close()

    async def _handshake(self, reader, writer) -> bool:
        try:
            raw = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=10)
        except Exception:
            return False
        lines = raw.decode("latin-1").split("\r\n")
        request = lines[0].split(" ")
        headers = {}
        for line in lines[1:]:
            if ":" in line:
                k, _, v = line.partition(":")
                headers[k.strip().lower()] = v.strip()
        if len(request) >= 2 and request[1].startswith("/health"):
            body = json.dumps({"service": "hermes-app", "protocol": PROTOCOL, "version": PLUGIN_VERSION}).encode()
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n"
                         + f"Content-Length: {len(body)}\r\n\r\n".encode() + body)
            await writer.drain()
            return False
        if len(request) >= 2 and request[1].startswith("/f/"):
            await self._serve_file(writer, request[1][3:].split("?", 1)[0], headers, head=request[0] == "HEAD")
            return False
        key = headers.get("sec-websocket-key")
        if headers.get("upgrade", "").lower() != "websocket" or not key:
            writer.write(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            await writer.drain()
            return False
        accept = base64.b64encode(hashlib.sha1((key + WS_GUID).encode()).digest()).decode()
        writer.write(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                      f"Sec-WebSocket-Accept: {accept}\r\n\r\n").encode())
        await writer.drain()
        return True

    async def _serve_file(self, writer, fid: str, headers: dict, head: bool = False) -> None:
        """GET /f/<id>: a file the agent sent. Needs the Hermes key as a bearer token; only files we stored."""
        def reply(status: str, body: bytes = b"", extra: str = "") -> bytes:
            return (f"HTTP/1.1 {status}\r\nConnection: close\r\nContent-Length: {len(body)}\r\n{extra}\r\n").encode() + body

        offered = headers.get("authorization", "")
        offered = offered[7:].strip() if offered.lower().startswith("bearer ") else ""
        if not offered or not any(hmac.compare_digest(offered.encode(), k.encode()) for k in self.tokens):
            writer.write(reply("401 Unauthorized"))
            await writer.drain()
            return
        found = _open_file(fid)
        if not found:
            writer.write(reply("404 Not Found"))
            await writer.drain()
            return
        blob, meta = found
        quoted = urllib.parse.quote(meta.get("name", "file"))
        head_text = ("HTTP/1.1 200 OK\r\nConnection: close\r\n"
                     f"Content-Type: {meta.get('mime', 'application/octet-stream')}\r\n"
                     f"Content-Length: {blob.stat().st_size}\r\n"
                     f"Content-Disposition: attachment; filename*=UTF-8''{quoted}\r\n\r\n")
        writer.write(head_text.encode())
        if not head:
            with open(blob, "rb") as fh:
                while True:
                    chunk = await asyncio.to_thread(fh.read, 65536)
                    if not chunk:
                        break
                    writer.write(chunk)
                    await writer.drain()
        await writer.drain()

    async def _hello(self, client: _Client, msg: dict) -> bool:
        offered = str(msg.get("token", "")).encode()
        if msg.get("t") != "hello" or not any(hmac.compare_digest(offered, k.encode()) for k in self.tokens):
            await client.send_json({"t": "error", "code": "unauthorized"})
            return False
        client.authed = True
        client.device = str(msg.get("device") or "")[:64]
        client.features = [str(f) for f in (msg.get("features") or [])][:20]
        after = msg.get("after")
        latest = _latest_seq()
        await client.send_json({"t": "ready", "protocol": PROTOCOL, "version": PLUGIN_VERSION, "features": FEATURES,
                                "latest": latest, "home": self._home_chat(),
                                "agents": self._live_agents(), "profiles": _profile_names()})
        if isinstance(after, int) and after < latest:
            for entry in _read_log(after):
                await client.send_json(entry)
        await client.send_json({"t": "synced", "latest": latest})
        return True

    def _home_chat(self) -> str:
        return str(_get_scoped_secret("HERMES_APP_HOME_CHANNEL", "") or HOME_CHAT)

    async def _handle_frame(self, client: _Client, msg: dict) -> None:
        kind = msg.get("t")
        if kind == "ping":
            await client.send_json({"t": "pong"})
        elif kind == "msg":
            await self._inbound(client, msg)
        elif kind == "update":
            await self._update_plugin(client)
        elif kind == "agents":
            await self._set_agents(client, msg)
        elif kind == "restart":
            await client.send_json({"t": "restarting"})
            _restart_gateway_soon()

    def _live_agents(self) -> dict:
        """{profile: chat id} for every agent the app can talk to over this link (read fresh, so it's always current)."""
        try:
            return _our_routes(_load_yaml((_home() / "config.yaml").read_text()))
        except Exception:  # noqa: BLE001
            return {}

    async def _set_agents(self, client: _Client, msg: dict) -> None:
        """Fixed action, only for an authenticated app: make exactly these profiles reachable, then restart so
        Hermes loads the routes. The app sends names only; they are checked against the profiles that exist."""
        wanted = msg.get("agents")
        if not isinstance(wanted, list):
            return
        result = await asyncio.to_thread(set_agents, [str(w) for w in wanted[:64]])
        await client.send_json(dict(result, t="agents_result"))
        if result.get("changed"):
            _restart_gateway_soon()

    async def _update_plugin(self, client: _Client) -> None:
        """Fixed action, only for an authenticated app: pull the newest plugin with Hermes's own command, then
        restart so the new code loads. Nothing the app sends is ever run as a command."""
        if self._updating:
            return
        self._updating = True
        try:
            before = _installed_version()
            try:
                proc = await asyncio.create_subprocess_exec(
                    *_hermes_cmd(), "plugins", "update", PLUGIN_NAME,
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
                out, _ = await asyncio.wait_for(proc.communicate(), timeout=120)
                ok = proc.returncode == 0
                detail = out.decode(errors="replace").strip()[-300:]
            except Exception as exc:  # noqa: BLE001 - reported to the app as text
                ok, detail = False, f"{type(exc).__name__}: {exc}"
            after = _installed_version()
            changed = ok and after and after != PLUGIN_VERSION
            await client.send_json({"t": "update_result", "ok": ok, "changed": bool(changed),
                                    "version": after or before, "detail": detail})
            if changed:
                _restart_gateway_soon()
        finally:
            self._updating = False

    async def _inbound(self, client: _Client, msg: dict) -> None:
        mid = str(msg.get("id") or uuid.uuid4().hex)
        chat = str(msg.get("chat") or self._home_chat())
        await client.send_json({"t": "ack", "id": mid})
        if mid in self._seen:
            return
        self._active_chats.add(chat)
        self._active_since[chat] = time.monotonic()
        self._seen[mid] = None
        while len(self._seen) > 500:
            self._seen.popitem(last=False)
        text = str(msg.get("text") or "")
        media_urls, media_types = [], []
        for img in (msg.get("images") or [])[:8]:
            path = self._save_image(img)
            if path:
                media_urls.append(str(path))
                media_types.append(str(img.get("mime") or "image/jpeg"))
        if not text and not media_urls:
            return
        source = self.build_source(chat_id=chat, chat_name="Hermes App", chat_type="dm",
                                   user_id="app-user", user_name="App")
        guidance = str(msg.get("guidance") or "").strip()[:2000] or None  # per-message hints from the app
        # What the app asked for this turn (the instruction, never the user's words), so "was it told to use voice?"
        # can be answered from the log.
        logger.info("Hermes App: turn in %s, instruction %s", chat,
                    "none" if not guidance else "%d chars: %r" % (len(guidance), guidance[:90]))
        event = MessageEvent(text=text, message_type=MessageType.PHOTO if media_urls else MessageType.TEXT,
                             source=source, message_id=mid, media_urls=media_urls, media_types=media_types,
                             channel_prompt=guidance, timestamp=datetime.datetime.now())
        await self.handle_message(event)

    @staticmethod
    def _save_image(img: dict) -> Optional[pathlib.Path]:
        try:
            data = base64.b64decode(img.get("b64", ""), validate=False)
        except Exception:
            return None
        if not data:
            return None
        ext = {"image/png": "png", "image/gif": "gif", "image/webp": "webp"}.get(str(img.get("mime")), "jpg")
        d = _data_dir() / "media"
        d.mkdir(exist_ok=True)
        path = d / f"{uuid.uuid4().hex}.{ext}"
        path.write_bytes(data)
        return path


# ── plugin registration ──────────────────────────────────────────────────────

# Hermes loads a separate copy of this module for each profile's plugin scope (a turn in one agent's profile runs the
# copy loaded for that profile), but only the gateway's copy owns the live adapter. So the reference to it lives on
# `sys`, which every copy in this process shares; a plain module global would be empty in all but one copy and the
# hooks below would silently do nothing for every other agent.
_ACTIVE: dict = sys.__dict__.setdefault("_hermes_app_active", {"adapter": None})


def _on_pre_tool_call(tool_name: str = "", args=None, **_kw):
    """Hermes hook: a tool is about to run. Report it to the phone as quiet activity. Never alters the call."""
    adapter = _ACTIVE.get("adapter")
    if adapter is not None and tool_name:
        with contextlib.suppress(Exception):
            adapter.emit_activity(str(tool_name), _brief(args))
    return None


_STREAM_EVENTS = ("on_stream_start", "on_stream_delta", "on_stream_end")


def _install_stream_tap() -> bool:
    """Listen to the model's words for every agent, not just the one this plugin is installed in.

    Hermes runs each agent profile in its own plugin scope and only calls hooks registered in that scope, so a hook
    registered here never hears a turn in another profile (and this plugin is installed once, in the main one). The
    stream events all pass through one function in the process, so this watches that function from the gateway: it
    adds a copy for our adapter and changes nothing about what Hermes does. Also tells Hermes someone is listening,
    which is what makes it hand out the words at all. Returns False (and the ordinary hooks are used instead, which
    cover the main agent only) if Hermes no longer has that function."""
    if sys.__dict__.get("_hermes_app_stream_tap"):
        return True
    try:
        import agent.plugin_stream_hooks as hooks
        original_enqueue, original_has = hooks.enqueue_plugin_stream_hook, hooks.has_stream_observer_hooks
    except Exception:  # noqa: BLE001 - not this Hermes
        return False

    def enqueue(hook_name, **payload):
        queued = original_enqueue(hook_name, **payload)
        if hook_name in _STREAM_EVENTS and payload.get("surface") == PLATFORM:
            adapter = _ACTIVE.get("adapter")
            if adapter is not None:
                with contextlib.suppress(Exception):
                    adapter.stream_event(hook_name, payload)
        return queued

    def has_observers():
        return bool(original_has()) or _ACTIVE.get("adapter") is not None

    hooks.enqueue_plugin_stream_hook = enqueue
    hooks.has_stream_observer_hooks = has_observers
    sys.__dict__["_hermes_app_stream_tap"] = True
    return True


def _stream_hook(event: str):
    """Hermes hook: the model is writing. Passes our own platform's words on to the phone as they come. Observer
    only: it never alters what Hermes does, and any trouble stays inside it."""
    def hook(**payload):
        adapter = _ACTIVE.get("adapter")
        if adapter is None:
            if payload.get("surface") == PLATFORM and not _ACTIVE.get("warned"):
                _ACTIVE["warned"] = True
                logger.info("Hermes App: stream hook ran but this process has no live adapter")
            return None
        if payload.get("surface") != PLATFORM:
            with contextlib.suppress(Exception):
                adapter._note_stream("surface:%s" % payload.get("surface"),
                                     "Hermes App: stream hook saw surface %r (not ours)" % (payload.get("surface"),))
            return None
        with contextlib.suppress(Exception):
            adapter._note_stream("ours", "Hermes App: stream hook is receiving this app's replies")
        with contextlib.suppress(Exception):
            adapter.stream_event(event, payload)
        return None
    hook.__name__ = event + "_hermes_app"
    return hook


def _hermes_cmd() -> list:
    """How to run the `hermes` command from inside the gateway, which may not have it on its PATH."""
    exe = shutil.which("hermes")
    return [exe] if exe else [sys.executable, "-m", "hermes_cli.main"]


def _installed_version() -> str:
    """Version in the plugin.yaml on disk (what is installed now), which differs from the running code after an update."""
    try:
        for line in (pathlib.Path(__file__).parent / "plugin.yaml").read_text().splitlines():
            if line.startswith("version:"):
                return line.split(":", 1)[1].strip().strip("\"'")
    except OSError:
        pass
    return ""


def _restart_gateway_soon(delay: int = 3) -> None:
    """Restart the gateway a moment from now, from a detached process (this one is about to be replaced)."""
    cmd = " ".join(shlex.quote(a) for a in _hermes_cmd() + ["gateway", "restart", "-y", "--detach"])
    subprocess.Popen(["sh", "-c", f"sleep {int(delay)}; {cmd}"], start_new_session=True,
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


# ── more than one agent: Hermes routes each app chat to a profile ────────────
# One gateway serves every profile. The app gives each agent its own chat id and Hermes's own
# `gateway.profile_routes` (in the default profile's config.yaml) sends that chat to the profile.
# We only ever add or remove entries we named ourselves (ROUTE_PREFIX...), by editing the file's text so
# comments and the user's other routes stay exactly as they were, and we refuse to write unless the
# result re-parses to the same config plus our routes.

ROUTE_PREFIX = "hermes-app-"
BACKUP_NAME = "config.yaml.hermes-app.bak"
_PROFILE_NAME = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")


def _chat_for_profile(name: str) -> str:
    return f"agent-{name}"


def _profile_names() -> list:
    """Profiles that exist on this machine (the default one is always served at `home`, so it isn't listed)."""
    root = _home() / "profiles"
    try:
        return sorted(p.name for p in root.iterdir() if p.is_dir() and _PROFILE_NAME.match(p.name))
    except OSError:
        return []


def _load_yaml(text: str):
    try:  # Hermes's own reader: its Python may not expose a plain `yaml` module
        from utils import fast_safe_load as load
    except ImportError:
        import yaml
        load = yaml.safe_load
    data = load(text)
    return data if isinstance(data, dict) else {}


def _our_routes(config: dict) -> dict:
    """{profile: chat id} for the hermes_app routes this plugin manages."""
    gw = config.get("gateway")
    routes = (gw or {}).get("profile_routes") if isinstance(gw, dict) else None
    out = {}
    for r in routes if isinstance(routes, list) else []:
        if isinstance(r, dict) and r.get("platform") == PLATFORM and str(r.get("name", "")).startswith(ROUTE_PREFIX) \
                and r.get("profile") and r.get("chat_id"):
            out[str(r["profile"])] = str(r["chat_id"])
    return out


def _route_block(name: str, indent: str) -> list:
    return [f"{indent}- name: {ROUTE_PREFIX}{name}", f"{indent}  platform: {PLATFORM}",
            f"{indent}  chat_id: {_chat_for_profile(name)}", f"{indent}  profile: {name}"]


def _indent_of(line: str) -> int:
    return len(line) - len(line.lstrip(" "))


def _edit_routes_text(text: str, wanted: list) -> str:
    """Return config text whose hermes-app routes are exactly `wanted`; everything else is left byte for byte."""
    lines = text.split("\n")
    # 1. drop our existing entries (a "- name: hermes-app-x" item and its indented fields)
    keep, i = [], 0
    while i < len(lines):
        m = re.match(r"^(\s*)-\s+name:\s*['\"]?" + re.escape(ROUTE_PREFIX), lines[i])
        if m:
            item = len(m.group(1))
            i += 1
            while i < len(lines) and (not lines[i].strip() or _indent_of(lines[i]) > item) and not lines[i].lstrip().startswith("- "):
                if not lines[i].strip():
                    break
                i += 1
            continue
        keep.append(lines[i])
        i += 1
    lines = keep
    if not wanted:
        return "\n".join(lines)
    # 2. find where to put the new ones
    for idx, line in enumerate(lines):
        m = re.match(r"^(\s+)profile_routes:\s*(\[\s*\])?\s*(#.*)?$", line)
        if m:
            key_indent = len(m.group(1))
            item_indent = " " * (key_indent + 2)
            end = idx + 1
            last_item = idx
            while end < len(lines):
                ln = lines[end]
                if ln.strip() and not ln.lstrip().startswith("#"):
                    ind = _indent_of(ln)
                    if ind < key_indent or (ind == key_indent and not ln.lstrip().startswith("- ")):
                        break
                    item_indent = " " * ind if ln.lstrip().startswith("- ") and ind >= key_indent else item_indent
                    last_item = end
                end += 1
            block = [l for n in wanted for l in _route_block(n, item_indent)]
            if m.group(2):  # `profile_routes: []`
                lines[idx] = f"{m.group(1)}profile_routes:" + (f" {m.group(3)}" if m.group(3) else "")
            lines[last_item + 1:last_item + 1] = block
            return "\n".join(lines)
    for idx, line in enumerate(lines):
        if re.match(r"^gateway:\s*(#.*)?$", line):
            child = next((_indent_of(l) for l in lines[idx + 1:] if l.strip() and not l.lstrip().startswith("#")), 2)
            ind = " " * (child if child > 0 else 2)
            block = [f"{ind}profile_routes:"] + [l for n in wanted for l in _route_block(n, ind + "  ")]
            lines[idx + 1:idx + 1] = block
            return "\n".join(lines)
    while lines and not lines[-1].strip():
        lines.pop()
    lines += ["gateway:", "  profile_routes:"] + [l for n in wanted for l in _route_block(n, "    ")] + [""]
    return "\n".join(lines)


def _strip_ours(config: dict) -> dict:
    """The config with our routes taken out, to prove an edit changed nothing else."""
    import copy
    c = copy.deepcopy(config)
    gw = c.get("gateway")
    if isinstance(gw, dict) and "profile_routes" in gw:
        if isinstance(gw["profile_routes"], list):
            gw["profile_routes"] = [r for r in gw["profile_routes"]
                                    if not (isinstance(r, dict) and str(r.get("name", "")).startswith(ROUTE_PREFIX))]
        if not gw["profile_routes"]:  # an empty or bare `profile_routes:` means the same as none
            gw.pop("profile_routes")
        if not gw:
            c.pop("gateway")
    return c


def _set_env_line(env_path: pathlib.Path, key: str, value: Optional[str]) -> bool:
    """Set (or with None, remove) one KEY=value line in a .env file, leaving every other line alone."""
    try:
        old = env_path.read_text() if env_path.exists() else ""
    except OSError:
        return False
    lines = [l for l in old.split("\n") if not l.startswith(key + "=")]
    while lines and not lines[-1].strip():
        lines.pop()
    if value is not None:
        lines.append(f"{key}={value}")
    new = "\n".join(lines) + "\n" if lines else ""
    if new == old:
        return False
    tmp = env_path.with_name(env_path.name + ".tmp")
    tmp.write_text(new)
    with contextlib.suppress(OSError):
        os.chmod(tmp, 0o600)
    os.replace(tmp, env_path)
    return True


def set_agents(wanted: list) -> dict:
    """Make the profiles in `wanted` reachable from the app, and only those. Returns a result for the app.
    Writes nothing unless the edited file re-parses to the same config plus exactly our routes."""
    available = _profile_names()
    names = sorted({str(n) for n in wanted if str(n) in available})
    skipped = sorted({str(n) for n in wanted} - set(names) - {"default"})
    path = _home() / "config.yaml"
    try:
        text = path.read_text() if path.exists() else ""
        before = _load_yaml(text)
    except Exception as exc:  # noqa: BLE001 - reported as text
        return {"ok": False, "changed": False, "agents": {}, "skipped": skipped,
                "detail": f"Couldn't read Hermes's settings file: {exc}"}
    current = _our_routes(before)
    result = {"ok": True, "changed": False, "agents": {n: _chat_for_profile(n) for n in names}, "skipped": skipped, "detail": ""}
    if sorted(current) != names or any(current[n] != _chat_for_profile(n) for n in current if n in names):
        new_text = _edit_routes_text(text, names)
        try:
            after = _load_yaml(new_text)
        except Exception:  # noqa: BLE001
            after = None
        if after is None or _our_routes(after) != {n: _chat_for_profile(n) for n in names} or _strip_ours(after) != _strip_ours(before):
            return dict(result, ok=False, agents=current,
                        detail="Hermes's settings file has a layout I don't dare edit, so nothing was changed.")
        try:
            if path.exists():
                shutil.copy2(path, path.with_name(BACKUP_NAME))
            tmp = path.with_name(path.name + ".tmp")
            tmp.write_text(new_text)
            with contextlib.suppress(OSError):
                os.chmod(tmp, path.stat().st_mode & 0o777 if path.exists() else 0o600)
            os.replace(tmp, path)
        except OSError as exc:
            return dict(result, ok=False, agents=current, detail=f"Couldn't save Hermes's settings file: {exc}")
        result["changed"] = True
    # scheduled messages from an agent should land in that agent's own chat
    for n in available:
        with contextlib.suppress(OSError):
            if _set_env_line(_home() / "profiles" / n / ".env", "HERMES_APP_HOME_CHANNEL",
                             _chat_for_profile(n) if n in names else None):
                result["changed"] = True
    return result


def _port(value) -> int:
    try:
        p = int(value)
        return p if 0 < p < 65536 else DEFAULT_PORT
    except (TypeError, ValueError):
        return DEFAULT_PORT


def _accepted_keys(extra: Optional[dict] = None) -> list:
    """Keys the app may present: the Hermes API key it chats with, and the older dedicated key if present."""
    keys = [str(_get_scoped_secret("API_SERVER_KEY", "") or "").strip(),
            str(_get_scoped_secret("HERMES_APP_TOKEN", "") or (extra or {}).get("token") or "").strip()]
    return [k for k in keys if k]


def check_requirements() -> bool:
    return bool(_accepted_keys())


def validate_config(config) -> bool:
    return bool(_accepted_keys(getattr(config, "extra", {}) or {}))


def _env_enablement() -> Optional[dict]:
    """Turn the platform on once the setup command has set HERMES_APP_ENABLED (or an older install's key exists)."""
    on = str(_get_scoped_secret("HERMES_APP_ENABLED", "") or "").strip().lower() in ("1", "true", "yes")
    if not (on or _get_scoped_secret("HERMES_APP_TOKEN", "")):
        return None
    seed = _seed_extra_from_env((
        ("HERMES_APP_PORT", "port", int), ("HERMES_APP_HOST", "host", None),
    ), home_env="HERMES_APP_HOME_CHANNEL", home_default=HOME_CHAT)
    return dict(seed)


async def _standalone_send(pconfig, chat_id: str, message: str, *, thread_id: Optional[str] = None,
                           media_files=None, force_document: bool = False) -> dict:
    """Cron without a co-resident gateway: write to the log (and copy any files); the running gateway pushes it to
    the phone and serves the files."""
    chat = str(chat_id or HOME_CHAT)
    try:
        if not media_files:
            entry = _append({"t": "msg", "id": uuid.uuid4().hex, "chat": chat, "text": message})
            return {"success": True, "message_id": entry["id"]}
        entry = None
        problems = []
        text = message
        for item in media_files:
            path, voice = (item[0], bool(item[1])) if isinstance(item, (list, tuple)) else (item, False)
            try:
                att = await asyncio.to_thread(_store_file, path, voice)
            except FileRejected as exc:
                problems.append(str(exc))
                continue
            entry = _append({"t": "msg", "id": uuid.uuid4().hex, "chat": chat, "kind": "reply",
                             "text": text, "attachments": [att]})
            text = ""  # the message text rides with the first file only
        if entry is None:  # every file was refused: say so instead of dropping the delivery silently
            entry = _append({"t": "msg", "id": uuid.uuid4().hex, "chat": chat,
                             "text": "\n".join([message] + problems).strip(), "kind": "status"})
            return {"error": "; ".join(problems) or "no files delivered"}
        return {"success": True, "message_id": entry["id"]}
    except OSError as exc:
        return {"error": f"Hermes App log not writable: {exc}"}


# What the agent is told about this surface (part of its system prompt, so Hermes caches it: it costs almost nothing
# per message). The fenced-code rule matters most for voice: Hermes leaves fenced blocks out of a spoken reply but
# speaks a command written inline or on a bare line.
PLATFORM_HINT = (
    "You are chatting with the user through their own Hermes mobile app on their phone. "
    "Markdown is supported. You may send follow-up messages later (for example when a long task "
    "finishes or a scheduled job runs); they arrive on the phone right away. Deliver files by writing "
    "MEDIA:/absolute/path/to/file on its own line: images, audio, video and documents (up to 25 MB) arrive as "
    "attachments in the chat. Remote images render via ![alt](url). "
    "Always put terminal commands and code in a fenced code block (three backticks on their own lines), never inline "
    "in a sentence and never on a bare line: the app shows a copyable block, and replies read aloud skip fenced "
    "blocks. Introduce it in words (\"run this:\") and keep the sentence around it free of the command itself."
)


TOOLSET_NAME = "hermes-" + PLATFORM  # the name Hermes looks up for a platform with no saved tool list


def _ensure_toolset():
    """Gives the app's chats the same tools a Telegram chat gets (voice replies, files, memory...).

    Hermes picks a chat's tools from a toolset named after its platform; with none defined the app's agents silently
    got almost no tools (no text_to_speech, so no voice notes). Defined in memory at startup: nothing is written to
    any profile's config, a toolset the user defined themselves is left alone, and each profile's own disabled
    toolsets still apply on top. Returns what happened, for the log and the tests."""
    try:
        import toolsets
    except Exception:
        return "no-toolsets-module"
    try:
        if toolsets.TOOLSETS.get(TOOLSET_NAME):
            return "already-defined"
        if toolsets.TOOLSETS.get("hermes-telegram"):
            toolsets.create_custom_toolset(
                TOOLSET_NAME, "Hermes App chats: the same full toolset as a personal Telegram chat",
                includes=["hermes-telegram"])
            return "included-telegram"
        core = list(getattr(toolsets, "_HERMES_CORE_TOOLS", []))
        if core:
            toolsets.create_custom_toolset(TOOLSET_NAME, "Hermes App chats: the core tools", tools=core)
            return "core-tools"
    except Exception:
        logger.warning("Hermes App: could not define the app toolset", exc_info=True)
    return "failed"


def register(ctx):
    logger.info("Hermes App: toolset %s", _ensure_toolset())
    entry = dict(
        name=PLATFORM,
        label="Hermes App",
        adapter_factory=HermesAppAdapter,
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=validate_config,
        required_env=["API_SERVER_KEY"],
        install_hint="No extra packages needed (stdlib only)",
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="HERMES_APP_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        allow_all_env="HERMES_APP_ALLOW_ALL_USERS",
        emoji="📱",
        pii_safe=False,
        # The app shows tool activity itself (from the hook below), so Hermes's text progress lines are off by
        # default for this platform. A user's own display settings still win.
        display_overrides={"tool_progress": "off", "show_reasoning": False},
        platform_hint=PLATFORM_HINT)
    try:
        ctx.register_platform(**entry)
    except TypeError:  # an older Hermes that doesn't know a newer option: register without the extras
        entry.pop("display_overrides", None)
        ctx.register_platform(**entry)
    with contextlib.suppress(Exception):
        ctx.register_hook("pre_tool_call", _on_pre_tool_call)
    if _install_stream_tap():
        logger.info("Hermes App: streaming tap installed")
    else:
        for event in _STREAM_EVENTS:
            with contextlib.suppress(Exception):  # an older Hermes without these hooks just doesn't stream
                ctx.register_hook(event, _stream_hook(event))
