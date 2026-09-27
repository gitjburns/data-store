#!/bin/sh

# Match release server processes, including those from other checkouts.
pgrep -f '/target/release/[d]ata-store-service([[:space:]]|$)' >/dev/null
case $? in
    0) echo "Service is running." ;;
    1) echo "Service is not running."; exit 1 ;;
    *) echo "Cannot check service status." >&2; exit 2 ;;
esac
