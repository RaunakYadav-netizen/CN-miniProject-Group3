# Nexus Chat — Distributed Chat with Leader Election

A dark browser UI for the existing Python TCP chat cluster, with automatic client reconnection after a server connection is lost.

## What changed in this version

- The existing TCP backend (`server_final.py`) and its Bully election, heartbeat detection, replicated user directory, and chat routing are preserved.
- The browser detects a dropped TCP session and automatically tries the other cluster servers first, then cycles through the configured servers with short delays.
- The username is kept in the active browser session during reconnect attempts. On successful login, the backend registers the user on the new server and its existing leader-coordinated registry logic updates the directory.
- The UI shows a **Reconnecting…** state while it attempts recovery.
- Users are not automatically moved back to their original server when it recovers. There is no automatic load rebalancing.

**Limitations:** this is reconnection, not a migration of the original TCP connection. A new TCP connection is created. Chat messages sent while disconnected are not persisted or replayed, and this project does not implement authentication-backed durable sessions. The username remains in browser memory while the page is open; a full page reload requires a new login.

## Files

```text
final_project/
├── server_final.py       # Existing distributed TCP backend (preserved)
├── client_final.py       # Existing terminal client (preserved)
├── web_gateway.py        # FastAPI/WebSocket-to-TCP bridge
├── requirements.txt      # Gateway dependencies
├── README.md
└── web_ui/
    └── index.html        # Dark browser UI + automatic reconnection
```

## Run on Fedora/Linux

### 1. Install dependencies

Run these commands from the `final_project` directory:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

If you already have a working virtual environment, activate it and install `requirements.txt` there instead.

### 2. Start the three TCP servers

Open three terminals in the project directory and run one command per terminal:

```bash
python server_final.py 1
```

```bash
python server_final.py 2
```

```bash
python server_final.py 3
```

Wait for the servers to discover each other and elect a leader.

### 3. Start the web gateway

In a fourth terminal:

```bash
source .venv/bin/activate
python -m uvicorn web_gateway:app --host 127.0.0.1 --port 8000 --reload
```

Open **http://127.0.0.1:8000** in a browser. Connect with a username and choose a server. Use separate tabs and different usernames to test cross-server chat.

## Test automatic reconnection

1. Connect a browser user through Server 3.
2. Connect at least one other user through a different server so you can observe the global directory.
3. Stop Server 3 (Ctrl+C in its terminal, or terminate its process).
4. The browser should display **Reconnecting…** and try Server 1 and Server 2 before retrying Server 3.
5. Once a healthy server accepts the username, the UI should show **Reconnected** and update the global user directory through the backend's existing registry protocol.
6. Start Server 3 again. It can rejoin the cluster and may reclaim leadership under the existing Bully election rules because it has the highest ID. The reconnected browser user remains on the server it reconnected to; this UI does not move them back to Server 3.

Failure detection and stale username reservation cleanup are asynchronous, so recovery may take several seconds. If the same username is still reserved while the failed server is being detected, the UI keeps retrying rather than requiring a manual login.

## Existing features

- Global chat (`/all`)
- Cross-server private messages (`/msg`)
- Global user directory (`/users`)
- Cluster health (`/status`)
- Leader information (`/leader`)
- Bully leader election and heartbeat-based peer failure detection

## Troubleshooting

- **Server unreachable:** confirm the corresponding TCP process is listening on port `5001`, `5002`, or `5003`.
- **Cannot reach the web UI:** confirm the gateway is running and visit `http://127.0.0.1:8000`.
- **Username rejected:** the backend enforces cluster-wide username uniqueness. During failover, wait for the cluster to detect the failed server and clear its stale registration.
- **Reconnect keeps retrying:** check the terminal logs for the TCP servers and gateway. Reconnection requires at least one reachable server and a username the cluster can accept.

The gateway is bound to `127.0.0.1` for local development. It is not configured for public/internet deployment.
