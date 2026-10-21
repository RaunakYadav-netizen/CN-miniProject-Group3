"""Browser-to-TCP bridge for the existing Distributed Chat backend.

This file does not replace or modify server.py. It connects each browser
session to one existing TCP chat server as an ordinary chat client.
"""
import asyncio
import re
from pathlib import Path
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

BASE_DIR = Path(__file__).resolve().parent
WEB_DIR = BASE_DIR / "web_ui"
HOST = "127.0.0.1"
SERVER_IDS = {1, 2, 3}

app = FastAPI(title="Nexus Chat Web Gateway")
app.mount("/assets", StaticFiles(directory=WEB_DIR), name="assets")


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(WEB_DIR / "index.html")


def parse_server_line(raw: str) -> dict[str, Any]:
    """Translate the existing line-based TCP protocol into UI events."""
    if "|" not in raw:
        return {"type": "info", "text": raw}

    kind, _, payload = raw.partition("|")
    if kind == "CHAT":
        parts = raw.split("|", 3)
        if len(parts) == 4:
            try:
                server_id = int(parts[1])
            except ValueError:
                server_id = None
            return {"type": "chat", "serverId": server_id, "username": parts[2], "text": parts[3]}
    elif kind == "PRIVATE":
        parts = raw.split("|", 2)
        if len(parts) == 3:
            return {"type": "private", "username": parts[1], "text": parts[2]}
    elif kind == "PRIVATE_SENT":
        parts = raw.split("|", 2)
        if len(parts) == 3:
            return {"type": "private_sent", "target": parts[1], "text": parts[2]}
    elif kind == "SYSTEM":
        return {"type": "system", "text": payload}
    elif kind == "SUCCESS":
        return {"type": "success", "text": payload}
    elif kind == "ERROR":
        return {"type": "error", "text": payload}
    elif kind == "USERS":
        return {"type": "users", "text": payload, "users": parse_users(payload)}
    elif kind == "STATUS":
        return {"type": "status", "text": payload, "servers": parse_status(payload)}
    elif kind == "LEADER":
        match = re.search(r"Server\s+(\d+)", payload, re.IGNORECASE)
        return {"type": "leader", "text": payload, "leaderId": int(match.group(1)) if match else None}
    elif kind == "HELP":
        return {"type": "help", "text": payload}
    return {"type": "info", "text": raw}


def parse_status(text: str) -> list[dict[str, Any]]:
    servers: list[dict[str, Any]] = []
    for entry in text.split(";"):
        match = re.match(r"\s*Server\s+(\d+)\s*:\s*(.+?)\s*$", entry, re.IGNORECASE)
        if match:
            servers.append({"id": int(match.group(1)), "status": match.group(2).upper()})
    return servers


def parse_users(text: str) -> list[dict[str, Any]]:
    """Parse the current /users text format into per-server user lists."""
    result: list[dict[str, Any]] = []
    if "No users are currently registered" in text:
        return result
    text = text.removeprefix("Global users:").strip()
    for entry in text.split(";"):
        match = re.match(r"\s*Server\s+(\d+)\s*:\s*(.*?)\s*$", entry, re.IGNORECASE)
        if match:
            names = [name.strip() for name in match.group(2).split(",") if name.strip()]
            result.append({"id": int(match.group(1)), "users": names})
    return result


@app.websocket("/ws")
async def chat_socket(websocket: WebSocket) -> None:
    await websocket.accept()
    writer: asyncio.StreamWriter | None = None
    tcp_task: asyncio.Task | None = None

    async def send_event(event: dict[str, Any]) -> None:
        await websocket.send_json(event)

    try:
        # The first browser message establishes the ordinary TCP client session.
        request = await websocket.receive_json()
        if request.get("type") != "connect":
            await send_event({"type": "error", "text": "Connect to a server first."})
            return

        try:
            server_id = int(request.get("serverId", 1))
        except (TypeError, ValueError):
            server_id = 0
        username = str(request.get("username", "")).strip()
        if server_id not in SERVER_IDS:
            await send_event({"type": "error", "text": "Choose Server 1, 2, or 3."})
            return
        if not username or len(username) > 24 or "|" in username or "\n" in username or "\r" in username:
            await send_event({"type": "error", "text": "Enter a username of 1–24 characters without | or line breaks."})
            return

        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(HOST, 5000 + server_id), timeout=4
            )
        except (OSError, asyncio.TimeoutError):
            await send_event({"type": "error", "text": f"Server {server_id} is unreachable. Start its Python server first."})
            return

        # Match the exact handshake used by the existing terminal client.
        prompt = await asyncio.wait_for(reader.readline(), timeout=4)
        if not prompt:
            await send_event({"type": "error", "text": f"Server {server_id} closed the connection during login."})
            return
        if prompt.decode("utf-8", errors="replace").strip() != "USERNAME":
            await send_event(parse_server_line(prompt.decode("utf-8", errors="replace").strip()))
        writer.write((username + "\n").encode("utf-8"))
        await writer.drain()
        # Wait for the backend's actual login result before treating the browser
        # session as connected (the backend can reject a globally taken name).
        first_response = await asyncio.wait_for(reader.readline(), timeout=5)
        if not first_response:
            await send_event({"type": "error", "text": "The server closed the connection before confirming login."})
            return
        first_event = parse_server_line(first_response.decode("utf-8", errors="replace").rstrip("\r\n"))
        if first_event.get("type") == "error":
            await send_event(first_event)
            return
        await send_event({"type": "session", "serverId": server_id, "username": username})
        await send_event(first_event)

        async def read_tcp() -> None:
            while True:
                line = await reader.readline()
                if not line:
                    await send_event({"type": "disconnected", "text": "The TCP server closed this session."})
                    return
                raw = line.decode("utf-8", errors="replace").rstrip("\r\n")
                if raw:
                    await send_event(parse_server_line(raw))

        tcp_task = asyncio.create_task(read_tcp())
        while True:
            try:
                message = await websocket.receive_json()
            except WebSocketDisconnect:
                break
            kind = message.get("type")
            command: str | None = None
            if kind == "chat":
                text = str(message.get("text", "")).strip()
                if text:
                    command = f"/all {text}"
            elif kind == "private":
                target = str(message.get("target", "")).strip()
                text = str(message.get("text", "")).strip()
                if target and text and " " not in target and "|" not in target:
                    command = f"/msg {target} {text}"
                else:
                    await send_event({"type": "error", "text": "Choose a recipient and enter a message."})
            elif kind == "command":
                command_name = str(message.get("command", ""))
                if command_name in {"/users", "/status", "/leader", "/help", "/quit"}:
                    command = command_name
            if command and writer and not writer.is_closing():
                writer.write((command + "\n").encode("utf-8"))
                await writer.drain()
                if command == "/quit":
                    break
    except WebSocketDisconnect:
        pass
    except (asyncio.TimeoutError, OSError, UnicodeError) as exc:
        try:
            await send_event({"type": "error", "text": f"Connection problem: {exc}"})
        except Exception:
            pass
    finally:
        if tcp_task:
            tcp_task.cancel()
            try:
                await tcp_task
            except (asyncio.CancelledError, Exception):
                pass
        if writer:
            try:
                if not writer.is_closing():
                    writer.write(b"/quit\n")
                    await writer.drain()
                writer.close()
                await writer.wait_closed()
            except (OSError, asyncio.CancelledError):
                pass
