
import socket
import threading
import sys

HOST = "127.0.0.1"
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 5001


def receive_messages(reader):
    try:
        while True:
            message = reader.readline()

            if not message:
                print("\n[CONNECTION CLOSED]")
                break

            message = message.rstrip("\n")

            if message == "USERNAME":
                continue

            elif message.startswith("ERROR|"):
                print("\n[ERROR] " + message[6:])

            elif message.startswith("SUCCESS|"):
                print("\n" + message[8:])

            elif message.startswith("SYSTEM|"):
                print("\n[SYSTEM] " + message[7:])

            elif message.startswith("USERS|"):
                print("\n" + message[6:])

            elif message.startswith("STATUS|"):
                print("\n[CLUSTER STATUS]")
                print(message[7:])
            elif message.startswith("LEADER|"):
                print("\n[LEADER] " + message[7:])

            elif message.startswith("CHAT|"):
                parts = message.split("|", 3)

                if len(parts) == 4:
                    _, server_id, username, text = parts
                    print(
                        f"\n[Server {server_id}] "
                        f"[{username}] {text}"
                    )
                else:
                    print("\n[CHAT] " + message)

            elif message.startswith("PRIVATE|"):
                parts = message.split("|", 2)
                print(
                    f"\n[PRIVATE from {parts[1]}] {parts[2]}"
                )

            elif message.startswith("PRIVATE_SENT|"):
                parts = message.split("|", 2)
                print(
                    f"\n[PRIVATE to {parts[1]}] {parts[2]}"
                )

            elif message.startswith("HELP|"):
                print("\n" + message[5:])

    except (OSError, UnicodeError):
        pass


client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

try:
    client.connect((HOST, PORT))
except OSError as error:
    print(f"Could not connect to server: {error}")
    client.close()
    raise SystemExit(1)

reader = client.makefile("r", encoding="utf-8")

try:
    prompt = reader.readline()

    if prompt.rstrip("\n") != "USERNAME":
        print("Unexpected server response.")
        raise SystemExit(1)

    username = input("Enter username: ").strip()
    client.sendall((username + "\n").encode("utf-8"))

    response = reader.readline()

    if not response:
        print("Server closed the connection.")
        raise SystemExit(1)

    response = response.rstrip("\n")

    if response.startswith("ERROR|"):
        print("[ERROR] " + response[6:])
        raise SystemExit(1)

    if not response.startswith("SUCCESS|"):
        print("Unexpected login response.")
        raise SystemExit(1)

    print(response[8:])

    threading.Thread(
        target=receive_messages,
        args=(reader,),
        daemon=True
    ).start()

    while True:
        try:
            message = input("> ")
        except (EOFError, KeyboardInterrupt):
            message = "/quit"
            print()
            break

        if not message.strip():
            continue

        client.sendall((message + "\n").encode("utf-8"))

        if message.strip() == "/quit":
            break

except (OSError, UnicodeError):
    print("Connection lost.")

finally:
    try:
        client.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass

    reader.close()
    client.close()

print("Disconnected.")