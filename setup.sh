#!/bin/bash
set -euo pipefail

RUNNER_UID="911"

DATA_DIR="${1:-./data}"
LOGS_DIR="${2:-./logs}"

echo "Initializing $DATA_DIR"
mkdir -p "$DATA_DIR"
chown -R "$RUNNER_UID":"$RUNNER_UID" "$DATA_DIR"

echo "Initializing $LOGS_DIR"
mkdir -p "$LOGS_DIR"
chown -R "$RUNNER_UID":"$RUNNER_UID" "$LOGS_DIR"

echo "Done."
