#!/bin/sh -x

./target/release/data-store --config config.toml "$@"
