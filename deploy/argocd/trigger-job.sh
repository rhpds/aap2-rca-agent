#!/usr/bin/env bash
# Fire a one-off Job from the rhdp-rca-batch CronJob for testing, without
# waiting for the schedule or unsuspending the CronJob (which would drift
# from the values pinned in git).
#
# Usage: trigger-job.sh [namespace] [job-name]
set -euo pipefail

namespace="${1:-rhdp-argocd-test}"
name="${2:-rhdp-rca-manual-$(date +%Y%m%d-%H%M%S)}"

kubectl create job "$name" --from=cronjob/rhdp-rca-batch -n "$namespace"
echo
echo "Job $name created in $namespace. To stream its logs:"
echo "  kubectl logs -f -n $namespace job/$name"
