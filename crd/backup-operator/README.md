# PointBlank Backup Operator (`pb-backup-crd-code`)

A Kubernetes Operator built to provide zero-touch, fully automated database and volume backups to Garage S3.

Instead of manually writing CronJobs, managing S3 buckets, and juggling access keys for every stateful application in your cluster, this operator allows you to declare a simple `Backup` custom resource. The operator dynamically provisions the S3 infrastructure, securely manages the credentials, and drops a self-healing backup pipeline directly into your cluster.

## Architecture & Features

* **Zero-Touch S3 Provisioning:** Natively integrates with the Garage Admin API. When a `Backup` resource is created, the operator automatically creates the target S3 bucket, generates an access key, links the permissions, and securely injects a Kubernetes Secret into the target namespace.
* **Auto-Healing Credentials:** If a Kubernetes Secret is accidentally deleted, but the S3 key still exists in the Garage database, the operator securely wipes the orphaned key and regenerates fresh credentials automatically.
* **Dynamic Database Blueprints:** The operator natively embeds backup logic for multiple engines (PostgreSQL, MySQL, SQLite, MongoDB, Redis). It dynamically fetches the correct hot-copy and compression scripts via ConfigMaps.
* **Automated Retention Policies:** Context-aware retention logic directly trims older backups from the S3 bucket based on the declared `retentionDays`.

---

## Getting Started

### Prerequisites

* Go v1.24.6+
* Docker v17.03+
* Kubernetes v1.26+ cluster
* A running instance of Garage S3 (with Admin API accessible at `garage.garage.svc.cluster.local:3906`)

### Deployment

**1. Build and push your image to your registry:**

```sh
make docker-build docker-push IMG=celebi7110/pb-backup-operator:latest

```

**2. Deploy the Operator and CRDs to the cluster:**

```sh
make deploy IMG=celebi7110/pb-backup-operator:latest

```

> **Note:** The operator will automatically provision its required `backup-blueprint-*` ConfigMaps into the `garage` namespace immediately upon startup.

---

## Usage Examples

Define a `Backup` custom resource to trigger the automated pipeline.

### Basic Setup (SQLite / Vaultwarden)

This minimal configuration relies on the operator's default fallbacks. It will auto-create a bucket named `vaultwarden-prod-backup-backups` and inject a secret named `vaultwarden-prod-backup-s3-credentials`.

```yaml
apiVersion: core.pointblank.com/v1alpha1
kind: Backup
metadata:
  name: vaultwarden-prod-backup
  namespace: default
spec:
  schedule: "0 2 * * *"               # Run every day at 2:00 AM
  targetApp: "vaultwarden"            
  sourcePVCName: "vaultwarden-data"   # The PVC to mount and back up
  databaseType: "sqlite"              # The blueprint engine to use

```

### Comprehensive Setup (PostgreSQL)

This example utilizes custom overrides for storage targets, retention policies, and database credentials.

```yaml
apiVersion: core.pointblank.com/v1alpha1
kind: Backup
metadata:
  name: postgres-core-backup
  namespace: # same namespace as of pvc of which backup needs to be done
spec:
  # Core Setup
  schedule: "0 3 * * *"
  targetApp: "postgres-db"
  sourcePVCName: "postgres-data-pvc"
  databaseType: "postgres"
  
  # Storage & S3 Overrides
  bucketName: "custom-postgres-backups"
  retentionDays: 14                   # Auto-prune backups older than 14 days
  
  # Container Overrides
  mountPath: "/var/lib/postgresql/data"  
  image: "postgres:15-alpine"            
  
  # Injecting Database Credentials
  databaseEnv:
    - name: POSTGRES_DB
      value: "production_db"
    - name: POSTGRES_USER
      valueFrom:
        secretKeyRef:
          name: db-credentials
          key: username
    - name: POSTGRES_PASSWORD
      valueFrom:
        secretKeyRef:
          name: db-credentials
          key: password

```

### Spec Reference

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `schedule` | String | **Yes** | Standard Cron expression for execution. |
| `targetApp` | String | **Yes** | App name for pod affinity and log grouping. |
| `sourcePVCName` | String | **Yes** | The exact name of the PersistentVolumeClaim to back up. |
| `databaseType` | String | **Yes** | Engine blueprint (`sqlite`, `postgres`, `mysql`, `mongodb`, `redis`, `default`). |
| `bucketName` | String | No | Target S3 bucket. Defaults to `<backup-name>-backups`. |
| `credentialsSecret` | String | No | K8s Secret containing AWS keys. Auto-provisioned if left blank. |
| `retentionDays` | Integer | No | Days to keep backups. `0` disables auto-pruning. |
| `mountPath` | String | No | PVC mount location in the backup pod. Defaults to `/data`. |
| `image` | String | No | OCI image for the backup runner. Overrides blueprint defaults. |
| `databaseEnv` | []EnvVar | No | Native K8s environment variables injected into the runner. |

---

## Uninstalling

**1. Delete active backup pipelines:**

```sh
kubectl delete backup --all --all-namespaces

```

**2. Undeploy the controller and CRDs:**

```sh
make undeploy
make uninstall

```

## License

Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

```
[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

```

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
