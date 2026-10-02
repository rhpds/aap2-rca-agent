#!/usr/bin/env bash
set -euo pipefail

# Compatibility entry point. Batch coordination now lives in the Python
# Agent SDK orchestrator; keep this script so existing cron/manual invocations
# can migrate without changing their command line.
exec rca-batch "$@"
