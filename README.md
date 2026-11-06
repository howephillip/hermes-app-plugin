# Hermes App plugin

Lets the **Hermes mobile app** stay connected to your [Hermes Agent](https://hermes-agent.nousresearch.com),
the same way Telegram or WhatsApp do. Your agents can message you first: follow-ups, finished background
work, scheduled (cron) deliveries. Replies stream to your phone over one live connection, and if the phone
drops off, it catches up on what it missed when it reconnects.

It adds "Hermes App" as a messaging platform in your Hermes gateway. Plain Python standard library, no extra
packages.

## What you get

- **Follow-ups and scheduled messages.** Agents can message you first; cron deliveries arrive too.
- **Files, images and audio.** Anything an agent sends with a `MEDIA:/absolute/path` tag (or Hermes's own
  text-to-speech voice replies) arrives in the chat as an attachment. Files up to 25 MB; the plugin keeps a copy for
  a week so an agent deleting a temp file can't break delivery. Downloads need your Hermes key.
- **Quiet status.** Memory notices, tool activity and other status lines are labelled so the app shows them as small
  text and never reads them aloud.
- **One-tap updates** from the app (Settings › Live Connection).

## Install

The app shows you the exact command. It is:

```sh
hermes plugins install howephillip/hermes-app-plugin --force --enable --yes-deps
```

then it turns the platform on and restarts Hermes. You do not need to copy keys: the app proves who it is with
the Hermes API key it already uses to chat (`API_SERVER_KEY`).

Manual setup, if you prefer:

1. `hermes plugins install howephillip/hermes-app-plugin --enable`
2. Add to your Hermes `.env`:
   ```
   HERMES_APP_ENABLED=true
   HERMES_APP_ALLOW_ALL_USERS=true
   ```
   (`ALLOW_ALL_USERS` is safe here: only a client holding your Hermes API key can connect.)
3. `hermes gateway restart`

## Settings (all optional, in your Hermes `.env`)

| Variable | Default | Meaning |
|---|---|---|
| `HERMES_APP_PORT` | `8648` | Port the app connects to. Your phone must be able to reach it. |
| `HERMES_APP_HOST` | `0.0.0.0` | Address to listen on. |
| `HERMES_APP_HOME_CHANNEL` | `home` | Chat that scheduled jobs deliver to. |

## Check it works

On the Hermes machine: `curl http://localhost:8648/health` should print `{"service": "hermes-app", ...}`.

## Remove

```sh
hermes plugins disable hermes-app
hermes plugins remove hermes-app
```

and delete the `HERMES_APP_*` lines from your `.env`, then `hermes gateway restart`.

## Files

Files live in `<HERMES_HOME>/hermes-app/files/<id>/` and are served at `GET /f/<id>` on the same port, with
`Authorization: Bearer <your Hermes key>`. Ids are random 128-bit values, so only files the agent actually sent can be
fetched. Files older than 7 days, or beyond 1 GB in total, are removed oldest-first.

## Security

- The connection requires your Hermes API key; wrong keys are refused.
- It is not encrypted by itself (plain `ws://`), like Hermes' own API server. Use it on your home network or
  behind a VPN/tunnel such as Tailscale, or put a TLS proxy in front.
- Messages are kept in `<HERMES_HOME>/hermes-app/log.jsonl` (last 5000 entries) so a reconnecting phone can
  catch up.
- File downloads use the same key and the same (unencrypted unless you add TLS) connection as everything else.

## Protocol

JSON text frames over WebSocket. The client sends `hello {token, device, after, features}`, then `msg {id, chat, text,
images?, guidance?}`. The server sends `ready {version, features}`, replayed and live `msg {kind, attachments?}` /
`edit` frames (each with a `seq`), `activity`, `typing`, `done`, `ack`, and `synced`. `kind` is `reply` or `status`.
An app that does not announce the `files` feature receives a plain-text note instead of attachments.

MIT licensed.
