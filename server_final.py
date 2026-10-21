
import socket
import threading
import sys
import time

HOST = "127.0.0.1"

SERVER_ID = int(sys.argv[1]) if len(sys.argv) > 1 else 1

if SERVER_ID not in (1, 2, 3):
    raise SystemExit("Server ID must be 1, 2, or 3")

CLIENT_PORT = 5000 + SERVER_ID
PEER_PORT = 6000 + SERVER_ID

PEERS = {
    1: ("127.0.0.1", 6001),
    2: ("127.0.0.1", 6002),
    3: ("127.0.0.1", 6003),
}

HEARTBEAT_INTERVAL = 1
HEARTBEAT_TIMEOUT = 5

clients = {}
lock = threading.Lock()

membership_lock = threading.Lock()
start_time = time.monotonic()

# Last heartbeat time for each peer.
last_seen = {peer_id: None for peer_id in PEERS if peer_id != SERVER_ID}

# UNKNOWN until we hear from a peer; then ALIVE or SUSPECTED DOWN.
peer_status = {
    peer_id: "UNKNOWN" for peer_id in last_seen
}

# Bully election state.
leader_lock = threading.Lock()
leader_id = None

election_state_lock = threading.Lock()
election_in_progress = False
ok_received = threading.Event()
coordinator_received = threading.Event()
ELECTION_WAIT = 2.5
COORDINATOR_WAIT = 4.0

# Step 5: cluster-wide user directory. The elected leader processes changes
# and broadcasts registry updates; each server keeps a local mirror so that
# /users remains quick and can recover after a leader change.
registry_lock = threading.Lock()
global_users = {server_id: set() for server_id in PEERS}
# Authoritative username ownership, protected by registry_lock on the leader.
username_reservations = {}
username_request_lock = threading.Lock()
pending_username_requests = {}
username_request_counter = 0


def send_message(sock, message):
    try:
        sock.sendall((message + "\n").encode("utf-8"))
        return True
    except OSError:
        return False


def broadcast_local(message, exclude=None):
    with lock:
        recipients = [
            (name, sock)
            for name, sock in clients.items()
            if name != exclude
        ]

    for name, sock in recipients:
        if not send_message(sock, message):
            print(f"[WARNING] Failed to send to {name}")


def send_to_peer(peer_id, message, report_error=True):
    host, port = PEERS[peer_id]

    try:
        with socket.create_connection(
            (host, port), timeout=2
        ) as peer_sock:
            peer_sock.sendall(
                (message + "\n").encode("utf-8")
            )
            return True

    except OSError as error:
        if report_error:
            print(f"[PEER {peer_id}] Unreachable: {error}")
        return False


def broadcast_global(message):
    broadcast_local(message)

    for peer_id in PEERS:
        if peer_id != SERVER_ID:
            threading.Thread(
                target=send_to_peer,
                args=(peer_id, message),
                daemon=True
            ).start()


def get_leader_id():
    with leader_lock:
        return leader_id


def set_leader(new_leader_id):
    global leader_id
    with leader_lock:
        leader_id = new_leader_id


def apply_registry_set(server_id, username):
    """Apply a registry update locally without forwarding it."""
    if server_id not in PEERS or not username:
        return
    with registry_lock:
        global_users[server_id].add(username)


def apply_registry_delete(server_id, username):
    if server_id not in PEERS or not username:
        return
    with registry_lock:
        global_users[server_id].discard(username)


def apply_registry_remove_server(server_id):
    if server_id not in PEERS:
        return
    with registry_lock:
        global_users[server_id].clear()


def format_global_users():
    with registry_lock:
        snapshot = {
            server_id: sorted(usernames)
            for server_id, usernames in global_users.items()
        }

    entries = []
    for server_id in sorted(snapshot):
        usernames = snapshot[server_id]
        if usernames:
            entries.append(f"Server {server_id}: {', '.join(usernames)}")

    if not entries:
        return "No users are currently registered in the cluster."
    return "Global users: " + "; ".join(entries)


def broadcast_registry_update(message):
    for peer_id in PEERS:
        if peer_id != SERVER_ID:
            threading.Thread(
                target=send_to_peer,
                args=(peer_id, message),
                kwargs={"report_error": False},
                daemon=True
            ).start()


def rebuild_username_reservations():
    """Rebuild ownership hints from the replicated directory after election."""
    with registry_lock:
        username_reservations.clear()
        for server_id, usernames in global_users.items():
            for username in usernames:
                # Duplicate entries should not normally exist; keep the first
                # deterministic owner if recovering from an older inconsistent state.
                username_reservations.setdefault(username, server_id)


def leader_reserve_username(server_id, username):
    """Atomically reserve a globally unique username on the leader."""
    if get_leader_id() != SERVER_ID or server_id not in PEERS or not username:
        return False

    with registry_lock:
        if username in username_reservations or any(
            username in names for names in global_users.values()
        ):
            return False
        username_reservations[username] = server_id
        global_users[server_id].add(username)

    print(f"[REGISTRY] Reserved username {username} for Server {server_id}")
    broadcast_registry_update(f"REGISTRY_SET|{server_id}|{username}")
    return True


def claim_cluster_username(username):
    """Ask the current leader to atomically claim a username for this server."""
    global username_request_counter

    # Give the initial Bully election time to finish if a client connects early.
    deadline = time.monotonic() + 8
    leader = get_leader_id()
    while leader is None and time.monotonic() < deadline:
        time.sleep(0.1)
        leader = get_leader_id()

    if leader not in PEERS:
        return False, "No cluster leader is available. Please reconnect shortly."

    if leader == SERVER_ID:
        if leader_reserve_username(SERVER_ID, username):
            return True, ""
        return False, f"Username '{username}' is already taken in the cluster."

    with username_request_lock:
        username_request_counter += 1
        request_id = f"{SERVER_ID}-{username_request_counter}-{time.time_ns()}"
        event = threading.Event()
        pending_username_requests[request_id] = {"event": event, "result": None}

    sent = send_to_peer(
        leader,
        f"USERNAME_CHECK|{SERVER_ID}|{request_id}|{username}",
        report_error=False
    )
    if not sent:
        with username_request_lock:
            pending_username_requests.pop(request_id, None)
        return False, "Could not contact the cluster leader. Please try again."

    if not event.wait(4):
        with username_request_lock:
            pending_username_requests.pop(request_id, None)
        return False, "The cluster leader did not respond. Please try again."

    with username_request_lock:
        pending = pending_username_requests.pop(request_id, None)
    result = pending["result"] if pending else None
    if result == "OK":
        return True, ""
    if result == "TAKEN":
        return False, f"Username '{username}' is already taken in the cluster."
    return False, "Could not verify username availability. Please try again."


def handle_username_protocol(parts, message):
    """Handle atomic username checks and their responses between servers."""
    kind = parts[0]

    if kind == "USERNAME_CHECK":
        fields = message.split("|", 3)
        if len(fields) != 4 or get_leader_id() != SERVER_ID:
            return
        try:
            origin_server = int(fields[1])
        except ValueError:
            return
        request_id, username = fields[2], fields[3]
        if origin_server not in PEERS or origin_server == SERVER_ID or not username:
            return
        accepted = leader_reserve_username(origin_server, username)
        status = "OK" if accepted else "TAKEN"
        send_to_peer(
            origin_server,
            f"USERNAME_RESULT|{request_id}|{status}",
            report_error=False
        )
        return

    if kind == "USERNAME_RESULT" and len(parts) == 3:
        request_id, status = parts[1], parts[2]
        if status not in ("OK", "TAKEN"):
            return
        with username_request_lock:
            pending = pending_username_requests.get(request_id)
            if pending is not None:
                pending["result"] = status
                pending["event"].set()


def leader_process_user_event(action, server_id, username):
    """Only the current leader processes authoritative registry events."""
    if get_leader_id() != SERVER_ID:
        return

    if action == "JOIN":
        with registry_lock:
            changed = username not in global_users[server_id]
            # Keep the ownership hint aligned with the authoritative directory.
            username_reservations.setdefault(username, server_id)
            global_users[server_id].add(username)
        if changed:
            print(f"[REGISTRY] Added {username} on Server {server_id}")
            broadcast_registry_update(f"REGISTRY_SET|{server_id}|{username}")
    elif action == "LEAVE":
        with registry_lock:
            changed = username in global_users[server_id]
            if username_reservations.get(username) == server_id:
                username_reservations.pop(username, None)
            global_users[server_id].discard(username)
        if changed:
            print(f"[REGISTRY] Removed {username} from Server {server_id}")
            broadcast_registry_update(f"REGISTRY_DEL|{server_id}|{username}")


def leader_process_server_down(server_id):
    if get_leader_id() != SERVER_ID or server_id not in PEERS:
        return
    with registry_lock:
        stale_names = set(global_users[server_id])
        global_users[server_id].clear()
        for username in stale_names:
            if username_reservations.get(username) == server_id:
                username_reservations.pop(username, None)
    print(f"[REGISTRY] Removed stale users for Server {server_id}")
    broadcast_registry_update(f"REGISTRY_REMOVE_SERVER|{server_id}")


def report_user_event(action, username):
    """Update this server's mirror, then report the event to the leader."""
    leader = get_leader_id()
    if leader == SERVER_ID:
        # Let the leader apply the event so it can detect the change and
        # broadcast it to the other servers.
        leader_process_user_event(action, SERVER_ID, username)
    else:
        # Keep this server's local view responsive while reporting to leader.
        if action == "JOIN":
            apply_registry_set(SERVER_ID, username)
        else:
            apply_registry_delete(SERVER_ID, username)

        if leader in PEERS:
            send_to_peer(
                leader,
                f"USER_EVENT|{action}|{SERVER_ID}|{username}",
                report_error=False
            )
        # If no leader is known yet, local state is retained. A coordinator
        # announcement later triggers sync_local_users_to_leader().


def report_server_down(server_id):
    leader = get_leader_id()
    if leader == SERVER_ID:
        leader_process_server_down(server_id)
    elif leader in PEERS:
        if not send_to_peer(
            leader, f"SERVER_DOWN|{server_id}", report_error=False
        ):
            apply_registry_remove_server(server_id)
    else:
        apply_registry_remove_server(server_id)


def sync_local_users_to_leader(target_leader=None):
    """Re-register active local users after a leader is elected or changes."""
    leader = target_leader if target_leader is not None else get_leader_id()
    with lock:
        usernames = list(clients.keys())

    for username in usernames:
        if leader == SERVER_ID:
            leader_process_user_event("JOIN", SERVER_ID, username)
        elif leader in PEERS:
            send_to_peer(
                leader, f"USER_SYNC|{SERVER_ID}|{username}",
                report_error=False
            )


def handle_registry_message(parts):
    """Process leader registry messages and replicated directory updates."""
    kind = parts[0]

    if kind in ("REGISTRY_SET", "REGISTRY_DEL") and len(parts) == 3:
        try:
            server_id = int(parts[1])
        except ValueError:
            return
        if server_id not in PEERS or not parts[2]:
            return
        if kind == "REGISTRY_SET":
            apply_registry_set(server_id, parts[2])
        else:
            apply_registry_delete(server_id, parts[2])
        return

    if kind == "REGISTRY_REMOVE_SERVER" and len(parts) == 2:
        try:
            server_id = int(parts[1])
        except ValueError:
            return
        apply_registry_remove_server(server_id)
        return

    if kind == "USER_EVENT" and len(parts) == 4:
        action = parts[1]
        try:
            server_id = int(parts[2])
        except ValueError:
            return
        username = parts[3]
        if action not in ("JOIN", "LEAVE") or server_id not in PEERS or not username:
            return
        if get_leader_id() == SERVER_ID:
            leader_process_user_event(action, server_id, username)
        else:
            leader = get_leader_id()
            if leader in PEERS:
                send_to_peer(
                    leader, f"USER_EVENT|{action}|{server_id}|{username}",
                    report_error=False
                )
        return

    if kind == "USER_SYNC" and len(parts) == 3:
        try:
            server_id = int(parts[1])
        except ValueError:
            return
        username = parts[2]
        if server_id not in PEERS or not username:
            return
        if get_leader_id() == SERVER_ID:
            leader_process_user_event("JOIN", server_id, username)
        else:
            leader = get_leader_id()
            if leader in PEERS:
                send_to_peer(
                    leader, f"USER_SYNC|{server_id}|{username}",
                    report_error=False
                )
        return

    if kind == "SERVER_DOWN" and len(parts) == 2:
        try:
            server_id = int(parts[1])
        except ValueError:
            return
        if server_id not in PEERS:
            return
        if get_leader_id() == SERVER_ID:
            leader_process_server_down(server_id)
        else:
            leader = get_leader_id()
            if leader in PEERS:
                send_to_peer(
                    leader, f"SERVER_DOWN|{server_id}",
                    report_error=False
                )
            else:
                apply_registry_remove_server(server_id)


def announce_coordinator():
    """Declare this server the leader and tell the other servers."""
    global election_in_progress

    set_leader(SERVER_ID)
    rebuild_username_reservations()
    with election_state_lock:
        election_in_progress = False
    coordinator_received.set()

    print(f"[ELECTION] Server {SERVER_ID} is the COORDINATOR (leader)")

    for peer_id in PEERS:
        if peer_id != SERVER_ID:
            threading.Thread(
                target=send_to_peer,
                args=(peer_id, f"COORDINATOR|{SERVER_ID}"),
                kwargs={"report_error": False},
                daemon=True
            ).start()

    # Reconcile local active users with the newly established coordinator.
    threading.Thread(
        target=sync_local_users_to_leader,
        args=(SERVER_ID,), daemon=True
    ).start()


def start_election():
    """Run a basic Bully election: the highest reachable ID wins."""
    global election_in_progress

    with election_state_lock:
        if election_in_progress:
            return
        election_in_progress = True

    ok_received.clear()
    coordinator_received.clear()
    print(f"[ELECTION] Server {SERVER_ID} starts an election")

    higher_peers = [peer_id for peer_id in PEERS if peer_id > SERVER_ID]
    reachable_higher = False

    for peer_id in higher_peers:
        if send_to_peer(
            peer_id, f"ELECTION|{SERVER_ID}", report_error=False
        ):
            reachable_higher = True

    if not reachable_higher:
        announce_coordinator()
        return

    # A higher server should reply OK, then conduct its own election.
    if not ok_received.wait(ELECTION_WAIT):
        announce_coordinator()
        return

    # Someone higher responded; wait for its COORDINATOR announcement.
    if coordinator_received.wait(COORDINATOR_WAIT):
        return

    print(f"[ELECTION] No coordinator announcement received; retrying")
    with election_state_lock:
        election_in_progress = False
    time.sleep(0.2)
    start_election()


def handle_election_message(parts):
    """Handle Bully election messages without restarting elections unnecessarily."""
    global election_in_progress

    if len(parts) != 2:
        return

    try:
        sender_id = int(parts[1])
    except ValueError:
        return

    if sender_id not in PEERS or sender_id == SERVER_ID:
        return

    if parts[0] == "ELECTION":
        # Only a higher-ID server answers a lower-ID server.
        if SERVER_ID > sender_id:
            send_to_peer(sender_id, f"OK|{SERVER_ID}", report_error=False)

            # If we're already coordinator, reaffirm that fact rather than
            # starting a fresh election for every request from a lower server.
            if get_leader_id() == SERVER_ID:
                send_to_peer(
                    sender_id, f"COORDINATOR|{SERVER_ID}",
                    report_error=False
                )
            else:
                threading.Thread(target=start_election, daemon=True).start()

    elif parts[0] == "OK":
        if sender_id > SERVER_ID:
            ok_received.set()

    elif parts[0] == "COORDINATOR":
        if sender_id < SERVER_ID:
            # A lower-ID server cannot override this server. If we're already
            # coordinator, correct its stale announcement; otherwise elect.
            if get_leader_id() == SERVER_ID:
                send_to_peer(
                    sender_id, f"COORDINATOR|{SERVER_ID}",
                    report_error=False
                )
            else:
                threading.Thread(target=start_election, daemon=True).start()
            return

        current_leader = get_leader_id()
        if current_leader is not None and current_leader > sender_id:
            # Ignore a delayed announcement from a lower leader while the
            # higher leader is still known to be alive.
            with membership_lock:
                current_status = peer_status.get(current_leader)
            if current_status == "ALIVE":
                return

        set_leader(sender_id)
        if sender_id == SERVER_ID:
            rebuild_username_reservations()
        with election_state_lock:
            election_in_progress = False
        coordinator_received.set()

        # Print only when the known leader actually changes.
        if current_leader != sender_id:
            print(f"[ELECTION] Server {sender_id} is the current leader")
            threading.Thread(
                target=sync_local_users_to_leader,
                args=(sender_id,), daemon=True
            ).start()


def handle_peer(peer_sock, address):
    reader = peer_sock.makefile("r", encoding="utf-8")

    try:
        message = reader.readline().rstrip("\n")

        if not message:
            return

        parts = message.split("|", 3)

        # Heartbeat protocol: HEARTBEAT|server_id
        if len(parts) == 2 and parts[0] == "HEARTBEAT":
            try:
                peer_id = int(parts[1])
            except ValueError:
                return

            if peer_id not in last_seen:
                print(f"[PEER] Invalid heartbeat from {address}")
                return

            now = time.monotonic()

            with membership_lock:
                previous_status = peer_status[peer_id]
                last_seen[peer_id] = now
                peer_status[peer_id] = "ALIVE"

            if previous_status != "ALIVE":
                print(f"[MEMBERSHIP] Server {peer_id} is ALIVE")

            return

        # Leader-election protocol: ELECTION|id, OK|id, COORDINATOR|id
        if len(parts) == 2 and parts[0] in ("ELECTION", "OK", "COORDINATOR"):
            handle_election_message(parts)
            return

        # Cluster-wide unique username reservation protocol.
        if parts[0] in ("USERNAME_CHECK", "USERNAME_RESULT"):
            handle_username_protocol(parts, message)
            return

        # Step 5 registry protocol. The leader processes user events and
        # broadcasts REGISTRY_SET/DEL updates to every server's local mirror.
        if parts[0] in (
            "USER_EVENT", "USER_SYNC", "SERVER_DOWN",
            "REGISTRY_SET", "REGISTRY_DEL", "REGISTRY_REMOVE_SERVER"
        ):
            handle_registry_message(parts)
            return

        # Cross-server private message routing.
        # PRIVATE_ROUTE|origin_server|sender|target|text
        route_parts = message.split("|", 4)
        if len(route_parts) == 5 and route_parts[0] == "PRIVATE_ROUTE":
            _, origin_server_text, sender, target, private_text = route_parts
            try:
                origin_server = int(origin_server_text)
            except ValueError:
                return

            with lock:
                target_client = clients.get(target)

            if target_client is not None and send_message(
                target_client, f"PRIVATE|{sender}|{private_text}"
            ):
                result = "DELIVERED"
            else:
                result = "NOT_FOUND"

            # Report the result to the server where the sender is connected.
            send_to_peer(
                origin_server,
                f"PRIVATE_RESULT|{origin_server}|{sender}|{target}|{result}|{private_text}",
                report_error=False
            )
            return

        # PRIVATE_RESULT|origin_server|sender|target|status|text
        result_parts = message.split("|", 5)
        if len(result_parts) == 6 and result_parts[0] == "PRIVATE_RESULT":
            _, origin_text, sender, target, result, private_text = result_parts
            try:
                origin_server = int(origin_text)
            except ValueError:
                return

            if origin_server != SERVER_ID:
                return

            with lock:
                sender_client = clients.get(sender)

            if sender_client is not None:
                if result == "DELIVERED":
                    send_message(
                        sender_client,
                        f"PRIVATE_SENT|{target}|{private_text}"
                    )
                else:
                    send_message(
                        sender_client,
                        f"ERROR|User '{target}' is no longer connected."
                    )
            return

        # Chat protocol: CHAT|origin_server|username|text
        if len(parts) == 4 and parts[0] == "CHAT":
            _, origin_server, username, text = parts

            print(
                f"[SERVER {origin_server} -> SERVER {SERVER_ID}] "
                f"{username}: {text}"
            )

            # Deliver locally; do not forward again.
            broadcast_local(message)
            return

        print(f"[PEER] Invalid message from {address}")

    except (OSError, UnicodeError) as error:
        print(f"[PEER ERROR] {error}")

    finally:
        reader.close()
        peer_sock.close()


def peer_listener():
    peer_server = socket.socket(
        socket.AF_INET, socket.SOCK_STREAM
    )
    peer_server.setsockopt(
        socket.SOL_SOCKET, socket.SO_REUSEADDR, 1
    )
    peer_server.bind((HOST, PEER_PORT))
    peer_server.listen()

    print(f"[PEER] Listening on {HOST}:{PEER_PORT}")

    while True:
        sock, address = peer_server.accept()

        threading.Thread(
            target=handle_peer,
            args=(sock, address),
            daemon=True
        ).start()


def heartbeat_loop():
    while True:
        for peer_id in PEERS:
            if peer_id == SERVER_ID:
                continue

            send_to_peer(
                peer_id,
                f"HEARTBEAT|{SERVER_ID}",
                report_error=False
            )

        time.sleep(HEARTBEAT_INTERVAL)


def membership_monitor():
    while True:
        now = time.monotonic()
        newly_down = []

        with membership_lock:
            for peer_id in last_seen:
                timestamp = last_seen[peer_id]

                # If we've never heard from this peer, allow
                # the timeout period after this server starts.
                reference_time = (
                    timestamp if timestamp is not None
                    else start_time
                )

                if now - reference_time > HEARTBEAT_TIMEOUT:
                    if peer_status[peer_id] != "SUSPECTED DOWN":
                        peer_status[peer_id] = "SUSPECTED DOWN"
                        newly_down.append(peer_id)
                        print(
                            f"[MEMBERSHIP] Server {peer_id} "
                            f"is SUSPECTED DOWN"
                        )

        for peer_id in newly_down:
            threading.Thread(
                target=report_server_down,
                args=(peer_id,), daemon=True
            ).start()

        current_leader = get_leader_id()
        leader_is_down = False
        if current_leader is not None and current_leader != SERVER_ID:
            with membership_lock:
                leader_is_down = peer_status.get(current_leader) == "SUSPECTED DOWN"

        if current_leader is None or leader_is_down:
            threading.Thread(target=start_election, daemon=True).start()

        time.sleep(0.5)


def get_cluster_status():
    with membership_lock:
        statuses = {
            peer_id: peer_status[peer_id]
            for peer_id in peer_status
        }

    lines = [f"Server {SERVER_ID}: ALIVE"]

    for peer_id in sorted(statuses):
        lines.append(
            f"Server {peer_id}: {statuses[peer_id]}"
        )

    return "; ".join(lines)


def handle_client(client, address):
    username = None
    registered = False
    reader = client.makefile("r", encoding="utf-8")

    try:
        send_message(client, "USERNAME")
        username = reader.readline().strip()

        if not username or "|" in username:
            send_message(client, "ERROR|Invalid username.")
            return

        # Fast local rejection, followed by an authoritative cluster-wide
        # reservation at the leader. The leader's check-and-reserve is atomic,
        # so two servers cannot accept the same name concurrently.
        with lock:
            locally_taken = username in clients
        if locally_taken:
            send_message(client, "ERROR|Username already taken on this server.")
            return

        accepted, reason = claim_cluster_username(username)
        if not accepted:
            send_message(client, f"ERROR|{reason}")
            return

        with lock:
            if username in clients:
                # Extremely unlikely local race; release the reservation through
                # the normal LEAVE path before rejecting this connection.
                local_race = True
            else:
                clients[username] = client
                registered = True
                local_race = False

        if local_race:
            report_user_event("LEAVE", username)
            send_message(client, "ERROR|Username already taken on this server.")
            return

        print(
            f"[CONNECTED] {username} on Server {SERVER_ID} "
            f"from {address}"
        )

        send_message(
            client,
            f"SUCCESS|Welcome to Server {SERVER_ID}, {username}!"
        )

        report_user_event("JOIN", username)

        broadcast_local(
            f"SYSTEM|{username} joined Server {SERVER_ID}.",
            exclude=username
        )

        while True:
            data = reader.readline()

            if not data:
                break

            message = data.rstrip("\n")

            if not message:
                continue

            print(f"[{username}] {message}")

            if message == "/quit":
                break

            elif message == "/users":
                send_message(
                    client,
                    f"USERS|{format_global_users()}"
                )

            elif message == "/leader":
                current_leader = get_leader_id()
                if current_leader is None:
                    send_message(client, "LEADER|Leader not elected yet.")
                else:
                    send_message(
                        client,
                        f"LEADER|Current leader: Server {current_leader}"
                    )

            elif message == "/status":
                send_message(
                    client,
                    f"STATUS|{get_cluster_status()}"
                )

            elif message == "/all" or message.startswith("/all "):
                text = message[4:].strip()

                if text:
                    broadcast_global(
                        f"CHAT|{SERVER_ID}|{username}|{text}"
                    )

            elif message.startswith("/msg "):
                parts = message.split(" ", 2)

                if len(parts) < 3 or not parts[2].strip():
                    send_message(
                        client,
                        "ERROR|Usage: /msg username message"
                    )
                    continue

                target, text = parts[1], parts[2]

                # First check whether the recipient is connected locally.
                with lock:
                    target_client = clients.get(target)

                if target_client is not None:
                    if send_message(
                        target_client,
                        f"PRIVATE|{username}|{text}"
                    ):
                        send_message(
                            client,
                            f"PRIVATE_SENT|{target}|{text}"
                        )
                    else:
                        send_message(
                            client,
                            "ERROR|Could not deliver the message."
                        )
                    continue

                # Otherwise locate the recipient in the replicated global directory.
                with registry_lock:
                    target_server = next(
                        (server_id for server_id, usernames in global_users.items()
                         if target in usernames),
                        None
                    )

                if target_server is None:
                    send_message(
                        client,
                        f"ERROR|User '{target}' is not connected to the cluster."
                    )
                    continue

                # The directory tells us where the recipient is; send directly
                # to that server rather than routing all private messages via the leader.
                route_message = (
                    f"PRIVATE_ROUTE|{SERVER_ID}|{username}|{target}|{text}"
                )
                if not send_to_peer(
                    target_server, route_message, report_error=False
                ):
                    send_message(
                        client,
                        f"ERROR|Could not reach Server {target_server}."
                    )

            elif message == "/help":
                send_message(
                    client,
                    "HELP|/all message | /msg username message | "
                    "/users (global directory) | /status | /leader | /help | /quit"
                )

            else:
                send_message(
                    client,
                    "ERROR|Unknown command. Type /help."
                )

    except (OSError, UnicodeError) as error:
        print(f"[CLIENT ERROR] {username}: {error}")

    finally:
        if registered:
            with lock:
                if clients.get(username) is client:
                    del clients[username]

            report_user_event("LEAVE", username)

            broadcast_local(
                f"SYSTEM|{username} left Server {SERVER_ID}.",
                exclude=username
            )

            print(f"[DISCONNECTED] {username}")

        reader.close()
        client.close()


def client_listener():
    server = socket.socket(
        socket.AF_INET, socket.SOCK_STREAM
    )
    server.setsockopt(
        socket.SOL_SOCKET, socket.SO_REUSEADDR, 1
    )
    server.bind((HOST, CLIENT_PORT))
    server.listen()

    print(f"[CLIENT] Listening on {HOST}:{CLIENT_PORT}")

    while True:
        client, address = server.accept()

        threading.Thread(
            target=handle_client,
            args=(client, address),
            daemon=True
        ).start()


print("=" * 45)
print(f" DISTRIBUTED CHAT - SERVER {SERVER_ID}")
print("=" * 45)

threading.Thread(
    target=peer_listener, daemon=True
).start()

threading.Thread(
    target=heartbeat_loop, daemon=True
).start()

threading.Thread(
    target=membership_monitor, daemon=True
).start()

# Give all servers a moment to start, then elect the highest available ID.
threading.Timer(4.0, start_election).start()

client_listener()