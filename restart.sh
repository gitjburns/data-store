#!/bin/sh

# Do not start another server if the shutdown request failed.
./stop.sh || exit "$?"

# Wait until server processes exit before clearing logs and starting again.
echo "Waiting for server shutdown..."
while :; do
    pgrep -f '/target/release/[d]ata-store-service([[:space:]]|$)' >/dev/null
    case $? in
        0) sleep 1 ;;
        1) break ;;
        *) echo "Cannot check server processes; restart aborted." >&2; exit 1 ;;
    esac
done

./start.sh
