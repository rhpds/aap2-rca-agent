# CI testing flow

Two workflows run in CI:

- **`tests.yml`**: unit and integration tests. Runs on every push and every PR.
- **`batch-cronjob.yml`**: installs the real Helm chart on OpenShift and runs one batch from the actual CronJob. Everything it talks to is synthetic: a CI Postgres, a CI SSH jumpbox and generated job logs.

All CI names, ports, DB settings and the model live in `ci/ci.env`. The workflow loads it into the job environment and renders the manifests, Helm flags and settings JSON from it; `ci/setup-namespace.sh` sources it too.

## Overview

```mermaid
flowchart LR
    push([push to any branch]) --> tests[tests.yml]
    pr([pull_request]) --> tests
    tests --> unit[Unit tests<br/>Python 3.11 + 3.13]
    tests --> integ[Integration tests<br/>Postgres]

    pushOcp([push to ocp-testing]) --> cron[batch-cronjob.yml]
    label([PR labeled<br/>safe-to-test]) --> cron
    dispatch([workflow_dispatch]) --> cron
    cron --> cluster[(OpenShift namespace)]
```

## `tests.yml`

```mermaid
flowchart TD
    start([push / pull_request]) --> conc{"Concurrency group<br/>workflow + ref<br/>(cancel in-progress)"}
    conc --> U & I

    subgraph U["unit job (matrix: 3.11, 3.13)"]
        u1[checkout] --> u2[setup-python + pip cache]
        u2 --> u3["pip install -e .[dev]"]
        u3 --> u4["pytest -m 'not integration'"]
    end

    subgraph I["integration job (3.13)"]
        i1[checkout] --> i2[setup-python + pip cache]
        i2 --> i3["pip install -e .[dev]"]
        i3 --> i4[tests/run_integration_tests.sh]
    end
```

## `batch-cronjob.yml`

### Trigger, gate and setup

```mermaid
flowchart TD
    T1([push to ocp-testing]) --> G
    T2([pull_request_target: labeled]) --> G
    T3([workflow_dispatch]) --> G

    G{"event is pull_request_target<br/>and label is not safe-to-test?"}
    G -- yes --> SKIP([job skipped])
    G -- no --> C["concurrency group: batch-cronjob<br/>(queued, never cancelled)"]

    C --> CO["checkout<br/>PR head SHA, or github.sha"]
    CO --> SETUP

    subgraph SETUP["Set up cluster access and tools (one step)"]
        KC["Write kubeconfig<br/>from secrets.KUBECONFIG_B64"] --> NSK["namespace from kubeconfig context"]
        NSK --> NSE{"namespace empty?"}
        NSE -- no --> VA["kubectl get namespace<br/>(verify access)"]
        VA --> IMGD["export NS, IMAGE, SSHD_IMAGE<br/>(internal registry, $NS/…:latest)"]
        IMGD --> OC["install oc → $HOME/bin<br/>(added to GITHUB_PATH)"]
    end
    NSE -- yes --> FAIL1([fail])
    OC --> BUILD
```

### Building images and synthetic infrastructure

```mermaid
flowchart TD
    BUILD["apply ImageStream + BuildConfig<br/>oc start-build --from-dir=. --wait"]

    BUILD --> KEY["ssh-keygen ed25519<br/>→ secret rca-ci-jumpbox-key"]

    KEY --> SB1["oc new-build rca-ci-jumpbox<br/>oc start-build --from-dir=ci/images/sshd"]

    SB1 --> INFRA["apply ci/manifests/postgres.yaml<br/>apply ci/manifests/sshd.yaml (SSHD_IMAGE)<br/>rollout restart jumpbox (pick up new key)<br/>wait for both rollouts"]
    INFRA --> HELM
```

### Chart install, seeding and the batch run

```mermaid
flowchart TD
    HELM["Read ANTHROPIC_BASE_URL / API_KEY<br/>from litellm secret"] --> LLC{"both present?"}
    LLC -- no --> FAIL3([fail])
    LLC -- yes --> HI["jq: settings JSON from ci/ci.env<br/>(SOURCE_DB_* → localhost:15432 tunnel)<br/>helm upgrade --install -f ci/values-cronjob.yaml<br/>--set from ci/ci.env: model, parallelism, SSH endpoints<br/>(bastion / db-bastion / jumpbox → rca-ci-jumpbox:2222,<br/>db remote → rca-ci-postgres:5432)<br/>--set: litellm creds, settings JSON, ssh key"]

    HI --> WP["Start rca-ci-workspace pod<br/>(IMAGE, mounts $RELEASE-pvc at /workspace)<br/>wait Ready"]
    WP --> ST["copy seed_test_db.py into workspace pod<br/>SOURCE_DB_* from ci/ci.env passed inline<br/>(direct to rca-ci-postgres:5432)"]
    ST --> SEED["seed_test_db.py in workspace pod:<br/>write job_*.json.gz into JOB_LOGS_DIR,<br/>create tables, insert one event per job"]

    SEED --> RUN["kubectl create job rca-ci-cronjob-run<br/>--from=cronjob/$RELEASE-batch"]
    RUN --> WAIT
```

### What the batch Job does in the cluster

```mermaid
flowchart LR
    subgraph Job["rca-ci-cronjob-run pod"]
        INIT["init: setup-ssh<br/>write ssh config + key"] --> MAIN["rca-batch"]
    end
    MAIN -- "SSH tunnel" --> JB["rca-ci-jumpbox<br/>(sshd :2222)"]
    JB -- "port forward" --> PG[("rca-ci-postgres<br/>:5432")]
    MAIN -- "read logs" --> PVC[("$RELEASE-pvc<br/>/workspace/synthetic-logs")]
    MAIN -- "LLM calls" --> LLM["LiteLLM<br/>(ANTHROPIC_BASE_URL)"]
    MAIN -- "upload results<br/>over SSH" --> JB2["rca-ci-jumpbox<br/>/usr/local/mlflow/&lt;job_id&gt;"]
    MAIN -- "write report" --> REP[("/workspace/.rca/reports/<br/>batch_*.json")]
    MAIN -. "Splunk / GitHub / Jira<br/>not configured" .-> SKIPPED[steps 2 and 4 skipped]
```

### Waiting, verification and cleanup

```mermaid
flowchart TD
    WAIT["ci/wait_for_job.sh<br/>poll every 10s"] --> W1{"Job condition<br/>Failed=True?"}
    W1 -- yes --> WF["dump job logs"] --> FAIL4([fail])
    W1 -- no --> W2{"Complete=True?"}
    W2 -- yes --> REPORT
    W2 -- no --> W3{"past WAIT_TIMEOUT_SECS<br/>(default 1200)?"}
    W3 -- yes --> WF
    W3 -- no --> WAIT

    REPORT["Find newest batch_*.json in workspace pod<br/>copy to out/batch_report.json<br/>save job logs → out/batch.log"] --> VR["python3 ci/verify_report.py<br/>(structure + counts, not LLM text)"]
    VR -- fail --> FAIL5([fail])
    VR -- pass --> UP["List /usr/local/mlflow on jumpbox<br/>ANALYZED = job_ids with status 'analyzed'"]
    UP --> UPC{"every analyzed job_id<br/>has an upload dir?"}
    UPC -- no --> FAIL6([fail])
    UPC -- yes --> OK([success])

    OK & FAIL4 & FAIL5 & FAIL6 --> ART["always: upload out/ as artifact<br/>batch-cronjob-run"]
    ART --> CL{"vars.CI_SKIP_CLEANUP == 'true'?"}
    CL -- no --> CLN["helm uninstall<br/>delete job, workspace pod,<br/>postgres, jumpbox, key secret"]
    CL -- yes --> NOTE["::notice:: resources left<br/>for inspection"]
```
