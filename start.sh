#!/bin/sh -x

./build.sh
./target/release/data-store-service --config config.toml
