#!/usr/bin/env bash
# Packer provisioner: installs the `lego` ACME client binary, used by
# lore-cert-renew.sh for Let's Encrypt HTTP-01 issuance/renewal. Pinned to an
# exact release + checksum rather than "latest", matching this repo's own
# convention (proxy_base_version, image versions, etc.) of never floating a
# dependency version.
set -euo pipefail

LEGO_VERSION="5.4.1"
LEGO_SHA256="ebb33f1bead5a7c99dd46f1c5734b44cf1eab5b5c12faf397cd14d50a5916419"
LEGO_TARBALL="lego_v${LEGO_VERSION}_linux_amd64.tar.gz"
LEGO_URL="https://github.com/go-acme/lego/releases/download/v${LEGO_VERSION}/${LEGO_TARBALL}"

workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT

curl -fsSL -o "$workdir/$LEGO_TARBALL" "$LEGO_URL"

echo "${LEGO_SHA256}  ${workdir}/${LEGO_TARBALL}" | sha256sum -c -

tar -xzf "$workdir/$LEGO_TARBALL" -C "$workdir" lego

sudo install -m 0755 -o root -g root "$workdir/lego" /usr/local/bin/lego
