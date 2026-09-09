#!/usr/bin/env bash
# Packer provisioner: bakes the staged loreserver binary, systemd units, and
# bootstrap script into the image. Runs once, at build time, as the `packer`
# SSH user via sudo. Does not start lore.service — see lore-server.pkr.hcl
# for why.
set -euo pipefail

sudo groupadd --system lore || true
sudo useradd --system --no-create-home --shell /usr/sbin/nologin --gid lore lore || true

sudo install -d -m 0750 -o lore -g lore /etc/lore/config
sudo install -d -m 0700 -o lore -g lore /etc/lore/certs

sudo install -m 0755 -o root -g root /tmp/lore-server-release/loreserver /usr/local/bin/loreserver
sudo install -m 0755 -o root -g root /tmp/lore-bootstrap.sh /usr/local/bin/lore-bootstrap.sh

sudo install -m 0644 -o root -g root /tmp/lore.service /etc/systemd/system/lore.service
sudo install -m 0644 -o root -g root /tmp/lore-bootstrap.service /etc/systemd/system/lore-bootstrap.service

sudo systemctl daemon-reload
sudo systemctl enable lore-bootstrap.service
sudo systemctl enable lore.service

rm -rf /tmp/lore-server-release /tmp/lore.service /tmp/lore-bootstrap.service /tmp/lore-bootstrap.sh
