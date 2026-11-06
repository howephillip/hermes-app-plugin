# Hermes App plugin

Lets the **Hermes mobile app** stay connected to your [Hermes Agent](https://hermes-agent.nousresearch.com),
the same way Telegram or WhatsApp do. Your agents can message you first: follow-ups, finished background
work, scheduled (cron) deliveries. Replies stream to your phone over one live connection, and if the phone
drops off, it catches up on what it missed when it reconnects.

It adds "Hermes App" as a messaging platform in your Hermes gateway. Plain Python standard library, no extra
packages.

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

## Security

- The connection requires your Hermes API key; wrong keys are refused.
- It is not encrypted by itself (plain `ws://`), like Hermes' own API server. Use it on your home network or
  behind a VPN/tunnel such as Tailscale, or put a TLS proxy in front.
- Messages are kept in `<HERMES_HOME>/hermes-app/log.jsonl` (last 5000 entries) so a reconnecting phone can
  catch up.

## Protocol

JSON text frames over WebSocket. The client sends `hello {token, device, after}`, then `msg {id, chat, text,
images?, guidance?}`. The server sends `ready`, replayed and live `msg` / `edit` frames (each with a `seq`),
`typing`, `done`, `ack`, and `synced`.

MIT licensed.
