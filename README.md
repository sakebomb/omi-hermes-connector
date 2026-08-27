# omi-hermes-connector

A small, dependency-light bridge that connects an [Omi](https://omi.me) wearable to your own
self-hosted, OpenAI-compatible assistant ("Hermes" here — swap in any gateway). Omi is the ears;
your assistant is the brain.

It exposes an Omi **External Integration** connector with two useful paths:

- **Spoken conversation → task capture.** Omi's *Memory creation* webhook posts a finished
  conversation; the bridge asks your assistant to file a task (or skip). Sensitive conversations
  are dropped locally (see Privacy).
- **Ask your assistant, hands-reduced, via Omi Chat Tools.** Omi's chat LLM extracts a clean,
  complete question and calls the `ask_hermes` tool once, synchronously; the bridge proxies it to
  your assistant and returns the answer. This avoids parsing raw real-time transcript fragments
  (Omi streams one short segment per POST, so a wake phrase never arrives whole).

## Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| `GET`  | `/healthz` | Health check → `ok` |
| `GET`  | `/` | App Home landing page |
| `GET`  | `/.well-known/omi-tools.json` | Chat Tools manifest (Omi fetches on install) |
| `GET`  | `/omi/setup_completed` | Returns `{"is_setup_completed": true}` |
| `POST` | `/omi/conversation/<token>` | Memory-creation webhook → task capture |
| `POST` | `/omi/realtime/<token>` | Legacy real-time wake path (unreliable; superseded by Chat Tools) |
| `POST` | `/omi/tool/ask_hermes` | Chat Tools endpoint → `{"result": <answer>}` |

## Setup

1. `cp .env.example .env` and fill it in (gateway URL/key, a long random `OMI_WEBHOOK_TOKEN`,
   your `OMI_USER_UID`). Put any personal names/keywords to redact in `OMI_DENYLIST` — they stay
   out of source control on purpose.
2. Run the bridge behind TLS on a host of your choice, e.g. `https://<your-host>`:
   `python3 omi-bridge.py`
3. In the Omi app, create a connector with the **External Integration** capability:
   - **Trigger:** Memory creation → **Webhook URL** `https://<your-host>/omi/conversation/<token>`
   - **Setup Completed URL:** `https://<your-host>/omi/setup_completed`
   - **App Home URL:** `https://<your-host>`
   - **Chat Tools Manifest URL:** `https://<your-host>/.well-known/omi-tools.json`

## Privacy

Two guardrails keep sensitive audio from leaving the box:

- **Category gate** — Omi labels each conversation (`structured.category`); anything in
  `OMI_PRIVATE_CATEGORIES` (default `family,health,medical,personal,relationship`) is dropped
  before any extraction or outbound call.
- **Denylist** — a keyword denylist (built-in defaults + your `OMI_DENYLIST`) drops matching
  content. Dropped items are never forwarded, searched, or persisted.

Omi's inbound webhooks are unsigned, so the shared-secret token in the path is the guard for the
webhook routes; the Chat Tools endpoint is gated on the operator's Omi `uid`.

## License

MIT — see [LICENSE](LICENSE).
