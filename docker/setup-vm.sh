#!/usr/bin/env bash
# Run on the VM: sudo bash setup-vm.sh .env.example
set -euo pipefail

[[ $EUID == 0 ]] || { echo "Run with sudo." >&2; exit 1; }
example=${1:?Usage: sudo bash setup-vm.sh /path/to/.env.example}
[[ -f "$example" ]]
# shellcheck source=/dev/null
source /etc/os-release
[[ "$ID" == ubuntu && "$VERSION_ID" == 24.04 && $(dpkg --print-architecture) == amd64 ]] || {
    echo "This setup targets Ubuntu 24.04 x86." >&2
    exit 1
}

echo "Installing Docker Engine and Compose"
apt-get update
apt-get install -y ca-certificates curl gnupg
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
chmod a+r /etc/apt/keyrings/docker.asc
echo 'deb [arch=amd64 signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu noble stable' \
    > /etc/apt/sources.list.d/docker.list

if ! command -v gcloud > /dev/null; then
    curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg \
        | gpg --dearmor --yes -o /etc/apt/keyrings/google-cloud.gpg
    echo 'deb [signed-by=/etc/apt/keyrings/google-cloud.gpg] https://packages.cloud.google.com/apt cloud-sdk main' \
        > /etc/apt/sources.list.d/google-cloud-sdk.list
fi
apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
if ! command -v gcloud > /dev/null; then
    apt-get install -y google-cloud-cli
fi
systemctl enable --now docker
gcloud auth configure-docker us-east1-docker.pkg.dev --quiet

echo "Preparing private configuration files"
platform_dir=/opt/data-platform
install -d -m 700 "$platform_dir" "$platform_dir/releases" "$platform_dir/backups"
if [[ ! -f "$platform_dir/.env" ]]; then
    install -m 600 "$example" "$platform_dir/.env"
fi
for name in tunnel-token dashboard-url; do
    if [[ ! -f "$platform_dir/$name" ]]; then
        touch "$platform_dir/$name"
    fi
done
chmod 600 "$platform_dir/.env" "$platform_dir/dashboard-url"
# The connector is unprivileged; the parent directory keeps this private on the host.
chmod 644 "$platform_dir/tunnel-token"

echo "Preparing 4 GB swap for the 2 GB VM"
if [[ ! -e /swapfile ]]; then
    fallocate -l 4G /swapfile
    chmod 600 /swapfile
    mkswap /swapfile
fi
if ! swapon --show=NAME --noheadings | grep -qx /swapfile; then
    swapon /swapfile
fi
if ! grep -qE '^/swapfile[[:space:]]' /etc/fstab; then
    echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi

docker compose version
echo "Ready. Fill .env, tunnel-token, and dashboard-url in $platform_dir."
