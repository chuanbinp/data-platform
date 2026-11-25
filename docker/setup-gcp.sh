#!/usr/bin/env bash
# Run once from an authenticated Cloud Shell or administrator workstation.
set -euo pipefail

project=hpml-cp3541
vm=data-platform
zone=us-east1-c
region=us-east1
repository=chuanbinp/data-platform
pool=data-platform-github
deployer="github-deployer@$project.iam.gserviceaccount.com"
runtime="data-platform-vm@$project.iam.gserviceaccount.com"

gcp() {
    gcloud --project "$project" --quiet "$@"
}

# Check authentication before creating anything.
repo_id=$(gh api "repos/$repository" --jq .id)
owner_id=$(gh api "repos/$repository" --jq .owner.id)
project_number=$(gcp projects describe "$project" --format='value(projectNumber)')
gcp compute instances describe "$vm" --zone "$zone" > /dev/null

echo "Preparing registry and service accounts"
gcp services enable compute.googleapis.com artifactregistry.googleapis.com \
    iam.googleapis.com iamcredentials.googleapis.com sts.googleapis.com \
    iap.googleapis.com oslogin.googleapis.com

if ! gcp artifacts repositories describe data-platform --location "$region" > /dev/null 2>&1; then
    gcp artifacts repositories create data-platform --location "$region" --repository-format docker
fi
for name in github-deployer data-platform-vm; do
    if ! gcp iam service-accounts describe "$name@$project.iam.gserviceaccount.com" > /dev/null 2>&1; then
        gcp iam service-accounts create "$name"
    fi
done
gcp artifacts repositories add-iam-policy-binding data-platform --location "$region" \
    --member="serviceAccount:$deployer" --role=roles/artifactregistry.writer > /dev/null
gcp artifacts repositories add-iam-policy-binding data-platform --location "$region" \
    --member="serviceAccount:$runtime" --role=roles/artifactregistry.reader > /dev/null

echo "Configuring private VM administration"
identity=$(gcp compute instances describe "$vm" --zone "$zone" --format='value(serviceAccounts[0].email)')
if [[ "$identity" != "$runtime" ]]; then
    status=$(gcp compute instances describe "$vm" --zone "$zone" --format='value(status)')
    restart_vm() {
        if [[ "$status" == RUNNING ]]; then
            gcp compute instances start "$vm" --zone "$zone"
        fi
    }
    if [[ "$status" == RUNNING ]]; then
        gcp compute instances stop "$vm" --zone "$zone"
        trap restart_vm EXIT
    fi
    gcp compute instances set-service-account "$vm" --zone "$zone" \
        --service-account "$runtime" --scopes cloud-platform
    restart_vm
    trap - EXIT
fi
gcp compute instances add-metadata "$vm" --zone "$zone" --metadata enable-oslogin=TRUE
gcp compute instances add-iam-policy-binding "$vm" --zone "$zone" \
    --member="serviceAccount:$deployer" --role=roles/compute.osAdminLogin > /dev/null
gcp projects add-iam-policy-binding "$project" \
    --member="serviceAccount:$deployer" --role=roles/compute.viewer --condition=None > /dev/null
gcp iam service-accounts add-iam-policy-binding "$runtime" \
    --member="serviceAccount:$deployer" --role=roles/iam.serviceAccountUser > /dev/null

network=$(gcp compute instances describe "$vm" --zone "$zone" --format='value(networkInterfaces[0].network)')
if ! gcp compute firewall-rules describe data-platform-iap-ssh > /dev/null 2>&1; then
    gcp compute firewall-rules create data-platform-iap-ssh --network="${network##*/}" \
        --allow=tcp:22 --source-ranges=35.235.240.0/20 --target-service-accounts="$runtime"
fi

echo "Configuring GitHub keyless authentication"
condition="assertion.repository_id == '$repo_id' && assertion.repository_owner_id == '$owner_id' && assertion.ref == 'refs/heads/master'"
if ! gcp iam workload-identity-pools describe "$pool" --location global > /dev/null 2>&1; then
    gcp iam workload-identity-pools create "$pool" --location global
fi
if ! gcp iam workload-identity-pools providers describe github \
    --location global --workload-identity-pool "$pool" > /dev/null 2>&1; then
    gcp iam workload-identity-pools providers create-oidc github \
        --location global --workload-identity-pool "$pool" \
        --issuer-uri=https://token.actions.githubusercontent.com \
        --attribute-mapping='google.subject=assertion.sub,attribute.repository_id=assertion.repository_id' \
        --attribute-condition="$condition"
else
    gcp iam workload-identity-pools providers update-oidc github \
        --location global --workload-identity-pool "$pool" \
        --attribute-condition="$condition"
fi
gcp iam service-accounts add-iam-policy-binding "$deployer" \
    --role=roles/iam.workloadIdentityUser \
    --member="principalSet://iam.googleapis.com/projects/$project_number/locations/global/workloadIdentityPools/$pool/attribute.repository_id/$repo_id" > /dev/null

echo "Set the GitHub production environment variable GCP_WORKLOAD_IDENTITY_PROVIDER to:"
gcp iam workload-identity-pools providers describe github \
    --location global --workload-identity-pool "$pool" --format='value(name)'
echo "Then grant github-deployer IAP-secured Tunnel User on this VM in the IAP console."
