#!/usr/bin/env bash
# lore-cache-storage-init.sh — RAID-0 stripe any attached Local SSDs and
# mount the array at /var/lib/lore/cache, for the optional local cache tier
# of a `composite` immutable_store (a local cache in front of the durable
# GCS-backed lore-gcp plugin — see docs/reference/lore-server-config.md's
# "composite" store mode). Adapted from nonseastarq's steno-storage-init.sh,
# same idempotency guarantees:
#   - Cold boot (no array present)       -> create fresh RAID-0
#   - Live migration (array preserved)   -> assemble existing array
#   - Re-run on an already-mounted host  -> no-op
#   - No Local SSDs attached             -> fall back to the boot disk
#
# Unlike stenographer, this cache is genuinely optional — nothing breaks if
# immutable_store.mode never gets set to "composite" — so the no-SSD path
# degrades rather than warns loudly: the directory still exists (so a
# composite config pointing at it never hard-fails to start), it's just on
# the small boot disk instead of fast local storage.
set -euo pipefail

MOUNTPOINT=/var/lib/lore/cache
ARRAY=/dev/md/lore-cache
ARRAY_NAME=lore-cache

log() { echo "[lore-cache-storage] $*" >&2; }

if mountpoint -q "$MOUNTPOINT"; then
  log "$MOUNTPOINT already mounted; nothing to do"
  exit 0
fi

# Wait briefly for udev to populate the Local SSD device symlinks — on cold
# boot the kernel sees the NVMe devices fast, but the by-id symlinks come
# from udev rules that can race with us.
for i in 1 2 3 4 5; do
  shopt -s nullglob
  devs=(/dev/disk/by-id/google-local-nvme-ssd-*)
  shopt -u nullglob
  [ ${#devs[@]} -gt 0 ] && break
  log "waiting for google-local-nvme-ssd-* symlinks (attempt $i/5)"
  sleep 1
done

if [ ${#devs[@]} -eq 0 ]; then
  log "no Local SSDs present; using the boot disk at $MOUNTPOINT (fine unless immutable_store.mode = composite is configured, in which case it's a slower cache, not an incorrect one)"
  install -d -o lore -g lore "$MOUNTPOINT"
  exit 0
fi

log "found ${#devs[@]} Local SSD(s):"
for d in "${devs[@]}"; do log "  - $d"; done

# Try to assemble an existing array first (live migration case).
if [ ! -e "$ARRAY" ]; then
  log "attempting to assemble pre-existing array"
  mdadm --assemble --scan || true
fi

if [ ! -e "$ARRAY" ]; then
  log "creating fresh RAID-0 across ${#devs[@]} disk(s)"
  mdadm --create --verbose --run --force \
    --level=0 \
    --raid-devices="${#devs[@]}" \
    --name="$ARRAY_NAME" \
    "$ARRAY" "${devs[@]}"
fi

# ext4 with stripe alignment: mdadm's default chunk is 512 KB, so
# stride = chunk / fs-block = 128 (with 4 KB blocks), and stripe-width =
# stride * data-disks (every disk is a data disk in RAID-0).
if ! blkid "$ARRAY" >/dev/null 2>&1; then
  log "no filesystem on $ARRAY; mkfs.ext4"
  mkfs.ext4 -F \
    -E stride=128,stripe-width=$((128 * ${#devs[@]})) \
    -L lore-cache \
    "$ARRAY"
fi

mkdir -p "$MOUNTPOINT"
mount -o noatime,nodiratime "$ARRAY" "$MOUNTPOINT"
chown lore:lore "$MOUNTPOINT"
log "mounted $ARRAY at $MOUNTPOINT"
