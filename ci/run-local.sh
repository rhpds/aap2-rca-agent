#!/usr/bin/env bash
# Run the batch-analysis GitHub Actions workflows locally with act against the
# CRC OpenShift cluster.
#
# Usage:
#   ci/run-local.sh tier1     # batch analysis with synthetic jobs
#   ci/run-local.sh tier2     # the real chart-rendered CronJob
#
# Notes:
# - GITHUB_TOKEN is unset for the run: a host token would be used by act to
#   clone actions and is rejected by GitHub.
# - ubuntu-latest maps to -self-hosted so job steps run on this host: the CRC
#   API listens on 127.0.0.1 only, unreachable from act's Docker containers.
# - Prerequisites (one-time): rca-ci project with the claude-litellm-creds
#   secret, and both images built into the internal registry.
set -euo pipefail

cd "$(dirname "$0")/.."

TIER="${1:?usage: run-local.sh tier1|tier2}"
IMAGE="image-registry.openshift-image-registry.svc:5000/rca-ci/rhdp-rca-plugin:latest"
SSHD_IMAGE="image-registry.openshift-image-registry.svc:5000/rca-ci/rca-ci-jumpbox:latest"
mkdir -p /tmp/act-artifacts

case "$TIER" in
  tier1)
    WORKFLOW=.github/workflows/batch-openshift.yml
    EXTRA_INPUTS=()
    ;;
  tier2)
    WORKFLOW=.github/workflows/batch-cronjob.yml
    EXTRA_INPUTS=(--input release=rca-ci-cron)
    ;;
  *)
    echo "unknown tier: $TIER (expected tier1 or tier2)" >&2
    exit 2
    ;;
esac

exec env -u GITHUB_TOKEN act workflow_dispatch -W "$WORKFLOW" \
  -P ubuntu-latest=-self-hosted \
  -s KUBECONFIG_B64="$(base64 -w0 ~/.kube/config)" \
  --input namespace=rca-ci \
  --input image="$IMAGE" \
  --input sshd_image="$SSHD_IMAGE" \
  "${EXTRA_INPUTS[@]}" \
  --artifact-server-path /tmp/act-artifacts
