#!/bin/sh -x

# Stop on build failure so an old binary cannot recreate storage.
./build.sh || exit "$?"
./target/release/data-store-service --config config.toml --setup-storage
