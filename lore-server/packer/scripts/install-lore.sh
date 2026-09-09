#!/usr/bin/env bash
# Packer provisioner: bakes the staged loreserver binary, systemd units, and
# bootstrap scripts into the image. Runs once, at build time, as the
# `packer` SSH user via sudo. Does not start lore.service — see
# lore-server.pkr.hcl for why.
set -euo pipefail

# mdadm isn't in proxy-base (it's per-daemon there too — see stenographer's
# own packer config) — needed by lore-cache-storage-init.sh's RAID-0 across
# any attached Local SSDs. proxy-base's own last provisioner step deletes
# /var/lib/apt/lists/* to shrink the image, so any daemon that installs its
# own packages needs `apt-get update` first — confirmed the hard way via a
# failed local validation build ("Package 'mdadm' has no installation
# candidate"), mirroring how stenographer.pkr.hcl runs the same update
# before its own apt-get install.
sudo apt-get update -qq
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends mdadm

sudo groupadd --system lore || true
sudo useradd --system --no-create-home --shell /usr/sbin/nologin --gid lore lore || true

sudo install -d -m 0750 -o lore -g lore /etc/lore/config
sudo install -d -m 0700 -o lore -g lore /etc/lore/certs

sudo install -m 0755 -o root -g root /tmp/lore-server-release/loreserver /usr/local/bin/loreserver
sudo install -m 0755 -o root -g root /tmp/lore-bootstrap.sh /usr/local/sbin/lore-bootstrap.sh
sudo install -m 0755 -o root -g root /tmp/lore-cache-storage-init.sh /usr/local/sbin/lore-cache-storage-init.sh

sudo install -m 0644 -o root -g root /tmp/lore.service /etc/systemd/system/lore.service
sudo install -m 0644 -o root -g root /tmp/lore-bootstrap.service /etc/systemd/system/lore-bootstrap.service
sudo install -m 0644 -o root -g root /tmp/lore-cache-storage.service /etc/systemd/system/lore-cache-storage.service

sudo systemctl daemon-reload
sudo systemctl enable lore-cache-storage.service
sudo systemctl enable lore-bootstrap.service
sudo systemctl enable lore.service

rm -rf /tmp/lore-server-release /tmp/lore.service /tmp/lore-bootstrap.service /tmp/lore-bootstrap.sh \
  /tmp/lore-cache-storage.service /tmp/lore-cache-storage-init.sh
