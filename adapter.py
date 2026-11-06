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
import os
import pathlib
import shlex
import shutil
import struct
import subprocess
import sys
import time
import unicodedata
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
PLUGIN_VERSION = "1.2.0"  # keep in step with plugin.yaml; the app compares it to know when to offer an update
DEFAULT_PORT = 8648
PROTOCOL = 1
# What this plugin can do, announced to the app on connect so a newer app can tell what an older plugin lacks.
FEATURES = ["kinds", "activity", "update"]
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

    async def send_json(self, obj: dict) -> None:
        self.writer.write(_frame(0x1, json.dumps(obj, separators=(",", ":")).encode()))
        await self.writer.drain()


# ── what kind of message is this ─────────────────────────────────────────────

_PROGRESS_VERBS = frozenset((
    "Running", "Reading", "Writing", "Searching", "Fetching", "Editing", "Listing", "Browsing", "Using", "Calling",
    "Executing", "Patching", "Scanning", "Opening", "Creating", "Deleting", "Installing", "Checking", "Looking",
    "Analyzing", "Analysing", "Thinking", "Remembering", "Recalling", "Saving", "Updating", "Loading", "Querying",
    "Downloading", "Uploading", "Navigating", "Clicking", "Typing", "Taking", "Delegating", "Planning", "Testing",
    "Building", "Finding", "Grepping", "Globbing", "Viewing", "Copying", "Moving", "Fixing",
))


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
        if not rest or rest.split(" ", 1)[0] not in _PROGRESS_VERBS:
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
        self._kinds: "OrderedDict[str, str]" = OrderedDict()  # message id -> kind, so an edit keeps its message's kind
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._seen: "OrderedDict[str, None]" = OrderedDict()  # inbound ids already handled (client retries)

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
        mid = uuid.uuid4().hex
        self._remember_kind(mid, kind, metadata)
        entry = self._record({"t": "msg", "id": mid, "chat": str(chat_id), "text": content, "kind": kind})
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
        self._active_chats.discard(str(event.source.chat_id))
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

    async def _hello(self, client: _Client, msg: dict) -> bool:
        offered = str(msg.get("token", "")).encode()
        if msg.get("t") != "hello" or not any(hmac.compare_digest(offered, k.encode()) for k in self.tokens):
            await client.send_json({"t": "error", "code": "unauthorized"})
            return False
        client.authed = True
        client.device = str(msg.get("device") or "")[:64]
        after = msg.get("after")
        latest = _latest_seq()
        await client.send_json({"t": "ready", "protocol": PROTOCOL, "version": PLUGIN_VERSION, "features": FEATURES,
                                "latest": latest, "home": self._home_chat()})
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
        elif kind == "restart":
            await client.send_json({"t": "restarting"})
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

_ACTIVE: dict = {"adapter": None}


def _on_pre_tool_call(tool_name: str = "", args=None, **_kw):
    """Hermes hook: a tool is about to run. Report it to the phone as quiet activity. Never alters the call."""
    adapter = _ACTIVE.get("adapter")
    if adapter is not None and tool_name:
        with contextlib.suppress(Exception):
            adapter.emit_activity(str(tool_name), _brief(args))
    return None


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
    """Cron without a co-resident gateway: write to the log; the running gateway pushes it to the phone."""
    try:
        entry = _append({"t": "msg", "id": uuid.uuid4().hex, "chat": str(chat_id or HOME_CHAT), "text": message})
    except OSError as exc:
        return {"error": f"Hermes App log not writable: {exc}"}
    return {"success": True, "message_id": entry["id"]}


def register(ctx):
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
        platform_hint=(
            "You are chatting with the user through their own Hermes mobile app on their phone. "
            "Markdown is supported. You may send follow-up messages later (for example when a long task "
            "finishes or a scheduled job runs); they arrive on the phone right away."))
    try:
        ctx.register_platform(**entry)
    except TypeError:  # an older Hermes that doesn't know a newer option: register without the extras
        entry.pop("display_overrides", None)
        ctx.register_platform(**entry)
    with contextlib.suppress(Exception):
        ctx.register_hook("pre_tool_call", _on_pre_tool_call)
