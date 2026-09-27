#!/bin/sh -x

# Stop on build failure before clearing logs or starting an old binary.
./build.sh || exit "$?"
rm -f logs/*
./target/release/data-store-service --config config.toml
