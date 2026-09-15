#!/bin/sh -x

./build.sh
rm -f logs/*
./target/release/data-store-service --config config.toml
