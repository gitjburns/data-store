#!/bin/sh

for i in sources/*; do echo; date; echo "${i##*/}"; time ./client.sh --ingest "${i##*/}"; done
