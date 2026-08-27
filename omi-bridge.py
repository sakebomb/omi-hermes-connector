#!/usr/bin/env python3
"""Omi → Hermes bridge.

Receives Omi wearable webhooks and routes them into the Hermes gateway.

Two endpoints (POST, guarded by a shared-secret token in the path):
  /omi/memory/<token>    Memory-Creation webhook (post-conversation digest).
                         Non-private memos are forwarded to Hermes with a task
                         directive; Hermes' LLM + vikunja tool files a task (B1).
  /omi/realtime/<token>  Real-Time-Transcript webhook. Legacy F1 wake path — unreliable
                         because Omi streams one fragment per POST, so a wake phrase never
                         arrives whole. Superseded by Chat Tools below; kept for reference.

Plus F1 v2 — Omi Chat Tools (the reliable instant-answer path):
  GET  /.well-known/omi-tools.json   Public tool manifest Omi fetches on install.
  POST /omi/tool/ask_hermes          Omi's chat LLM calls this with a clean, complete
                                     `query`; we answer synchronously via Hermes and return
                                     {"result": <answer>}. Operator-gated by uid (E1'), E2 applied.

Guardrails:
  E1  Every request must carry the shared-secret token or it is rejected 401.
  E2  Content matching the privacy denylist (family/kid/health) is DROPPED:
      never forwarded, never web-searched, never persisted. Only a redacted
      marker is logged.
"""
import json
import logging
import os
import re
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx

# hermes_common (stdlib-only) provides zulip_dm; mounted read-only for the module only.
# Zulip/operator creds come from the container environment (compose), not proactive.env,
# because the bridge runs as UID 10000 and cannot read the root-owned secrets file.
sys.path.insert(0, "/opt/proactive")
from hermes_common import zulip_dm  # noqa: E402

log = logging.getLogger("omi-bridge")

# --- config ---------------------------------------------------------------
# zulip_dm() reads ZULIP_BOT_EMAIL/ZULIP_BOT_API_KEY/OPERATOR_EMAIL/ZULIP_SITE from this mapping.
ENV = os.environ
HERMES_URL = os.environ.get("HERMES_API_URL", "http://localhost:8642/v1")
HERMES_KEY = os.environ["HERMES_API_KEY"]
HERMES_HEADERS = {"Authorization": f"Bearer {HERMES_KEY}"}
WEBHOOK_TOKEN = os.environ["OMI_WEBHOOK_TOKEN"]
PORT = int(os.environ.get("OMI_BRIDGE_PORT", "8646"))

# F1 v2 — Omi Chat Tools (external_integration). Omi's own chat LLM extracts a clean, complete
# query and calls TOOL_PATH synchronously, expecting {"result": <answer>}. Replaces the fragile
# realtime wake-word path. Manifest is served (public) at /.well-known/omi-tools.json.
TOOL_PATH = "/omi/tool/ask_hermes"
TOOL_TIMEOUT = float(os.environ.get("OMI_TOOL_TIMEOUT", "60"))
# Chat Tools carries no path token/signature; Omi sends the caller's uid. If set, only the
# operator's own Omi account may invoke the tool (its uid is on every realtime `?uid=` too).
OMI_USER_UID = os.environ.get("OMI_USER_UID", "").strip()

OMI_TOOLS_MANIFEST = {
    "tools": [
        {
            "name": "ask_hermes",
            "description": (
                "Ask Hermes — the operator's personal homelab and life AI assistant — a question "
                "and get an answer back. Call this whenever the user wants Hermes to answer a "
                "question, look something up, check homelab/infrastructure status, or reason about "
                "a problem. Pass the user's full natural-language question as `query`."
            ),
            "endpoint": TOOL_PATH,
            "method": "POST",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The user's full natural-language question for Hermes.",
                    }
                },
                "required": ["query"],
            },
            "auth_required": False,
            "status_message": "Asking Hermes…",
        }
    ]
}

# Strict wake prefix (F1). Optional leading hey/ok/hi, then "hermes", then the question.
WAKE_RE = re.compile(
    r"^\s*(?:hey|hi|ok|okay)?\s*herm[eè]s\b[\s,:.-]*(?P<q>.+)$",
    re.IGNORECASE | re.DOTALL,
)

# E2 privacy denylist. Configurable via OMI_DENYLIST (comma-separated, adds to defaults).
_DEFAULT_DENY = [
    "my son", "my kid", "my wife", "my family",
    "health", "doctor", "diagnosis", "prescription", "medication",
    "therapy", "mental health", "symptom", "blood test",
]
DENYLIST = _DEFAULT_DENY + [
    w.strip().lower() for w in os.environ.get("OMI_DENYLIST", "").split(",") if w.strip()
]

# E2 (category signal): Omi assigns each conversation a `structured.category`. That label is
# the most reliable privacy gate — a family/health chat is dropped even when its summary never
# uses a denylisted phrase (e.g. "a child's sleep schedule" mentions no denylist token).
# Tunable via OMI_PRIVATE_CATEGORIES (comma-separated, replaces the default set).
PRIVATE_CATEGORIES = {
    c.strip().lower() for c in os.environ.get(
        "OMI_PRIVATE_CATEGORIES", "family,health,medical,personal,relationship"
    ).split(",") if c.strip()
}

# De-dupe repeated real-time fires (Omi streams overlapping transcript chunks).
_SEEN: dict[str, float] = {}
_SEEN_TTL = float(os.environ.get("OMI_DEDUP_TTL", "180"))

TASK_DIRECTIVE = (
    "You are processing a captured voice memo (post-conversation, spoken aloud by the "
    "operator). Do NOT use web search or create GitHub issues for this. If the memo contains "
    "a to-do, reminder, or task, create exactly ONE Vikunja task with your vikunja tool: infer "
    "the project, set a priority label (P0-P3), an @context label, a Kanban bucket, and a "
    "due_date if any time is mentioned. Then reply with a one-line 'Filed: <title> (<project>, "
    "due <date>)'. If it is a buildable product/feature idea, reply 'IDEA: <one-line>'. "
    "Otherwise reply exactly 'SKIP'.\n\nVoice memo:\n"
)


# --- helpers --------------------------------------------------------------
def is_private(text: str) -> bool:
    low = (text or "").lower()
    return any(term in low for term in DENYLIST)


def _dedup(key: str) -> bool:
    """Return True if key was seen within TTL; records it either way. Prunes old keys."""
    now = time.time()
    for k, ts in list(_SEEN.items()):
        if now - ts > _SEEN_TTL:
            _SEEN.pop(k, None)
    if key in _SEEN:
        return True
    _SEEN[key] = now
    return False


def hermes_chat(content: str, timeout: float = 120) -> str:
    """One-shot (stateless) call to the Hermes gateway; returns the reply text."""
    r = httpx.post(
        f"{HERMES_URL}/chat/completions",
        json={"model": "hermes-agent", "messages": [{"role": "user", "content": content}]},
        headers=HERMES_HEADERS,
        timeout=timeout,
    )
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"]


def dm(content: str) -> None:
    try:
        st, resp, _ = zulip_dm(ENV, content)
        if str(st) != "200":
            log.error("zulip_dm failed: %s %s", st, str(resp)[:200])
    except Exception as exc:  # noqa: BLE001
        log.error("zulip_dm error: %s", exc)


# --- webhook handlers -----------------------------------------------------
def _memory_text(payload: dict) -> str:
    """Build a text blob from an Omi Memory-Creation (completed-conversation) payload.

    Prefer the structured summary (title/overview/action_items) Omi already extracts;
    fall back to the raw transcript, then to legacy flat fields. Defensive because Omi's
    exact schema varies by app version — the raw keys are logged in handle_memory.
    """
    parts: list[str] = []
    st = payload.get("structured") or payload.get("summary_json") or {}
    if isinstance(st, dict):
        if st.get("title"):
            parts.append(str(st["title"]))
        if st.get("overview"):
            parts.append(str(st["overview"]))
        for a in st.get("action_items") or []:
            d = a.get("description") if isinstance(a, dict) else a
            if d:
                parts.append(f"- {d}")
    if not parts:
        segs = payload.get("transcript_segments") or payload.get("segments") or []
        if isinstance(segs, list):
            txt = " ".join(s.get("text", "") for s in segs if isinstance(s, dict)).strip()
            if txt:
                parts.append(txt)
    if not parts:
        for k in ("summary", "content", "text"):
            if payload.get(k):
                parts.append(str(payload[k]))
                break
    return "\n".join(p for p in parts if p).strip()


def handle_memory(payload: dict) -> None:
    cid = str(payload.get("id", "?"))
    # Idempotency: Omi re-delivers the same conversation (retries after a slow ack, plus
    # multiple processing-stage webhooks). Process each conversation id at most once per TTL
    # so retries never file duplicate tasks.
    if cid != "?" and _dedup(f"conv:{cid}"):
        log.info("memory %s: duplicate delivery, skipped", cid)
        return
    st = payload.get("structured") or payload.get("summary_json") or {}
    category = str(st.get("category", "")).strip().lower() if isinstance(st, dict) else ""
    # E2: drop sensitive Omi categories before extracting or forwarding anything.
    if category in PRIVATE_CATEGORIES:
        log.info("memory %s DROPPED by private category '%s'", cid, category)
        return
    text = _memory_text(payload)
    if not text:
        log.info("memory %s: no extractable text (category=%r, keys=%s)",
                 cid, category, sorted(payload.keys()))
        return
    if is_private(text):
        log.info("memory %s DROPPED by privacy denylist (%d chars)", cid, len(text))
        return
    try:
        reply = hermes_chat(TASK_DIRECTIVE + text).strip()
    except Exception as exc:  # noqa: BLE001
        log.error("hermes error (memory): %s", exc)
        return
    if reply and reply.upper() != "SKIP":
        log.info("memory %s -> %.80s", payload.get("id", "?"), reply)
        dm(f":memo: {reply}")


def _extract_text(payload: dict) -> str:
    if isinstance(payload.get("segments"), list):
        return " ".join(s.get("text", "") for s in payload["segments"] if isinstance(s, dict)).strip()
    return (payload.get("text") or "").strip()


def handle_realtime(payload: dict) -> None:
    text = _extract_text(payload)
    if not text:
        return
    m = WAKE_RE.match(text)
    if not m:
        return  # no wake phrase -> stay silent (keeps real-time low-noise)
    question = m.group("q").strip()
    if len(question) < 3:
        return
    key = f"{payload.get('conversation_id', '')}:{question.lower()[:80]}"
    if _dedup(key):
        return
    if is_private(question):
        log.info("wake question DROPPED by privacy denylist")
        dm(":lock: That sounded personal, so I didn't search the web or act on it. "
           "Ask me directly if you'd like.")
        return
    log.info("wake question -> %.80s", question)
    try:
        answer = hermes_chat(question).strip()
    except Exception as exc:  # noqa: BLE001
        log.error("hermes error (realtime): %s", exc)
        dm(f":warning: Hermes couldn't answer that: {exc}")
        return
    dm(f":ear: **{question}**\n\n{answer}")


# "conversation" is the current Omi name ("Conversation events"); "memory" kept as an alias.
ROUTES = {"conversation": handle_memory, "memory": handle_memory, "realtime": handle_realtime}


# --- HTTP server ----------------------------------------------------------
class Handler(BaseHTTPRequestHandler):
    server_version = "omi-bridge/1.0"

    def _send(self, code: int, body: str = "") -> None:
        data = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_json(self, code: int, obj: dict) -> None:
        data = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _handle_tool(self) -> None:
        """Chat Tools (F1 v2): SYNCHRONOUS — Omi's chat LLM sends a clean, complete query and
        waits for {"result": <answer>} to show the user. Unlike the fire-and-forget webhooks we
        MUST block on the gateway and return the answer in the body (that is the contract)."""
        length = int(self.headers.get("Content-Length", 0) or 0)
        raw = self.rfile.read(length) if length else b""
        try:
            payload = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            return self._send_json(400, {"error": "bad json"})
        if not isinstance(payload, dict):
            return self._send_json(400, {"error": "bad payload"})
        uid = str(payload.get("uid", ""))
        if OMI_USER_UID and uid != OMI_USER_UID:  # operator-only when configured
            log.info("tool ask_hermes rejected: uid %r != operator", uid[:12])
            return self._send_json(401, {"error": "unauthorized"})
        query = str(payload.get("query", "")).strip()
        if not query:
            return self._send_json(400, {"error": "empty query"})
        if is_private(query):  # E2
            log.info("tool ask_hermes DROPPED by privacy denylist")
            return self._send_json(200, {"result": "That sounded personal, so I didn't look it "
                                                    "up. Ask me directly if you'd like."})
        log.info("tool ask_hermes -> %.80s", query)
        try:
            answer = hermes_chat(query, timeout=TOOL_TIMEOUT).strip()
        except Exception as exc:  # noqa: BLE001
            log.error("hermes error (tool): %s", exc)
            return self._send_json(500, {"error": f"Hermes couldn't answer that: {exc}"})
        return self._send_json(200, {"result": answer})

    def log_message(self, format, *args):  # noqa: A002  route through logging module
        log.info("%s - %s", self.address_string(), format % args)

    def _match(self) -> str | None:
        """Return route name if path is /omi/<route>/<valid-token>, else None."""
        parts = self.path.split("?", 1)[0].strip("/").split("/")
        if len(parts) != 3 or parts[0] != "omi" or parts[1] not in ROUTES:
            return None
        if parts[2] != WEBHOOK_TOKEN:  # E1
            return None
        return parts[1]

    def do_GET(self):
        path = self.path.split("?", 1)[0]
        if path == "/healthz":
            return self._send(200, "ok")
        if path == "/.well-known/omi-tools.json":  # Chat Tools manifest (public; Omi fetches it)
            return self._send_json(200, OMI_TOOLS_MANIFEST)
        if path == "/omi/setup_completed":  # Omi GETs this to gate enabling; no setup needed
            return self._send_json(200, {"is_setup_completed": True})
        if path == "/":  # App Home URL — a reachable landing page for the connector
            return self._send(200, "Hermes × Omi connector — operational.")
        # Some webhook UIs probe with GET; accept a valid route+token as verification.
        return self._send(200, "ok") if self._match() else self._send(404, "not found")

    def do_POST(self):
        if self.path.split("?", 1)[0] == TOOL_PATH:  # Chat Tools: synchronous answer path
            return self._handle_tool()
        route = self._match()
        if route is None:
            return self._send(401, "unauthorized")
        length = int(self.headers.get("Content-Length", 0) or 0)
        raw = self.rfile.read(length) if length else b""
        try:
            payload = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            return self._send(400, "bad json")
        # Ack immediately, then do the (slow, up-to-120s LLM) work on a background thread.
        # Omi — like most webhook senders — times out in seconds and retries on a slow ack,
        # which caused duplicate deliveries. Decoupling the 200 from the work stops the retries
        # (and keeps the wake-word path low-latency).
        data = payload if isinstance(payload, dict) else {}
        threading.Thread(target=self._run_route, args=(route, data), daemon=True).start()
        return self._send(200, "ok")

    @staticmethod
    def _run_route(route: str, payload: dict) -> None:
        try:
            ROUTES[route](payload)
        except Exception as exc:  # noqa: BLE001
            log.error("handler %s crashed: %s", route, exc)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    log.info("omi-bridge listening on :%d (routes: %s; denylist terms: %d)",
             PORT, ", ".join(ROUTES), len(DENYLIST))
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
