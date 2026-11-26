# Deploy to GCP

The VM needs outbound internet access; no external IP or inbound web ports.
Replace `YOUR_PROJECT_ID` in the commands below with your GCP project ID.

```text
Browser → Cloudflare HTTPS → cloudflared → dashboard
Push to master → GitHub Actions → Artifact Registry → VM deployment through IAP
```

## 1. Set up GCP once

From this repository in Cloud Shell, with project administrator permissions and
both `gcloud` and `gh` authenticated:

```sh
bash docker/setup-gcp.sh
```

The script prepares the registry, service accounts, IAP firewall, and GitHub
keyless authentication. It briefly restarts a running VM when changing its
service account. Existing resources are reused on subsequent runs.

Complete these two settings:

- **GCP → Identity-Aware Proxy → SSH and TCP resources:** select `data-platform` and grant `github-deployer` **IAP-secured Tunnel User**.
- **GitHub → Settings → Environments → production:** add `GCP_WORKLOAD_IDENTITY_PROVIDER` with the value the script prints. Restrict deployments to `master`.

Ensure `master` is the GitHub default branch and protect it with the `test` check.
Your administrator account also needs [IAP and OS Login access](https://docs.cloud.google.com/compute/docs/connect/ssh-using-iap) to prepare the VM.

## 2. Prepare the VM once

Run from the same repository in Cloud Shell:

```sh
gcloud compute scp docker/setup-vm.sh .env.example data-platform: \
  --project=YOUR_PROJECT_ID --zone=us-east1-c --tunnel-through-iap
gcloud compute ssh data-platform \
  --project=YOUR_PROJECT_ID --zone=us-east1-c --tunnel-through-iap \
  --command='sudo bash setup-vm.sh .env.example'
```

This installs Docker and the Google Cloud CLI, configures registry access, and
adds 4 GB swap. Existing configuration files are preserved when rerun.

Connect to the VM using the same SSH command without `--command`, then edit:

```sh
sudoedit /opt/data-platform/.env
sudoedit /opt/data-platform/tunnel-token
sudoedit /opt/data-platform/dashboard-url
```

Fill every password, JWT secret, and EIA key in `.env`. The other files contain
only the tunnel token and `https://YOUR_HOSTNAME` respectively, without a trailing
slash. The setup script sets their permissions; keep the parent directory private.

## 3. Configure Cloudflare once

Use a domain managed in Cloudflare DNS, with your organization's approval.

1. Create a **Cloudflared** tunnel named `data-platform` in Cloudflare Zero Trust.
2. Put its token in the VM's `tunnel-token` file.
3. Add your hostname as a published application route to **HTTP → `dashboard:8080`**.
4. Enable edge HTTP-to-HTTPS redirection and keep `/api/*` uncached.

Keep visitor IP headers enabled and Pseudo IPv4 disabled. Outbound port `7844`
must be allowed. Only the dashboard is public; Airflow and Postgres stay private.

## 4. Deploy

Push to `master`, or choose **Actions → Test and deploy → Run workflow** on `master`.
Tests run first; Actions builds images and deploys them to the VM. Each deployment
backs up the databases, waits for active tasks, and checks dashboard health.
Before restarting services, deployment runs `dbt build` to build and test the
warehouse views against existing data.
The first deployment enables ingestion; later deployments preserve its paused state.

Open your dashboard URL and confirm both pipeline branches complete in Airflow.
For the Airflow UI, run this on your laptop, then open `http://localhost:8000`:

```sh
gcloud compute ssh data-platform --project=YOUR_PROJECT_ID --zone=us-east1-c \
  --tunnel-through-iap -- -N -L 8000:127.0.0.1:8000
```

To roll back, run on the VM:

```sh
sudo bash /opt/data-platform/previous/docker/deploy.sh
```

If a failed deployment never became current, use `current` instead of `previous`.
Rollback does not reverse database migrations. Private backups live under
`/opt/data-platform/backups`; copy them off the VM and periodically test restores.
Postgres uses a stable startup script outside release directories. If a future
release changes that script, deployment stops for a deliberate maintenance update.

For daily backups, seven-run cloud retention, and restore commands, follow the
[backup and recovery guide](backups.md). Deployment installs `jq` if needed, checks
bucket access before changing services, and enables the timer after health checks.
Run the updated GCP setup script before the first deployment of this feature.
After recovery,
manual Compose commands must use the shared production helper to load the volume
override, as shown in that guide.

The 2 GB VM runs one Airflow task and one dbt thread at a time. Check memory with
`free -h` and `sudo docker stats --no-stream`; resize if it runs out of memory.
