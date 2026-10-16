#!/usr/bin/env bash
# Runs INSIDE the batch analysis pod. Prepares the workspace the same way the
# CronJob's init container does (SSH config, Skill install), then runs
# rca-batch against the in-cluster CI Postgres. Job logs are read from
# JOB_LOGS_DIR on the workspace PVC and uploads go to the in-cluster CI
# jumpbox (sshd pod) — no bastion, no real jumpbox, no SSH outside the
# cluster.
set -euo pipefail
export HOME=/workspace

LIMIT="${RCA_CI_LIMIT:-10}"
SINCE_AGO="${RCA_CI_SINCE_AGO:-2 hours}"

: "${CI_JUMPBOX_HOST:?CI_JUMPBOX_HOST must be set}"
JUMP_PORT="${CI_JUMPBOX_PORT:-2222}"
JUMP_USER="${CI_JUMPBOX_USER:-testuser}"

mkdir -p "$HOME/.ssh"
cp /tmp/ci-jumpbox-key/id_ed25519 "$HOME/.ssh/id_ci_jumpbox"
chmod 600 "$HOME/.ssh/id_ci_jumpbox"
cat > "$HOME/.ssh/config" <<EOF
Host ci-jumpbox
  HostName $CI_JUMPBOX_HOST
  Port $JUMP_PORT
  User $JUMP_USER
  IdentityFile $HOME/.ssh/id_ci_jumpbox
  StrictHostKeyChecking no
  UserKnownHostsFile /dev/null
EOF
chmod 600 "$HOME/.ssh/config"

# Install the root-cause-analysis Skill (same exclusions as the CronJob init
# container; the claude CLI is already on PATH in the image).
mkdir -p "$HOME/.claude/skills"
rsync -a --exclude='.analysis' --exclude='__pycache__' --exclude='*.pyc' --exclude='tests' --exclude='README.md' \
  /opt/app/skills/ "$HOME/.claude/skills/"
echo "[RUN] Skills installed: $(ls "$HOME/.claude/skills/" | tr '\n' ' ')"

SINCE=$(date -u -d "$SINCE_AGO ago" "+%Y-%m-%d %H:%M:%S")
echo "[RUN] rca-batch --limit $LIMIT --since $SINCE"
exec rca-batch --limit "$LIMIT" --since "$SINCE"
