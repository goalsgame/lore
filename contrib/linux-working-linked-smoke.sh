#!/usr/bin/env bash
# Read/edit/reuse a pinned link through the actual Linux CLI and FUSE driver.
# Usage: $0 /path/to/lore /normal/checkout EXACT_ROOT_HASH linked/relative-file [source-check-file]
# A reproducible same-repository historical-link fixture is emitted by:
# LORE_KEEP_TEST_DATA=1 cargo test -p lore --test unit --features test-util \
#   working_mount_factory::policy::linked_reuse_requires_current_policy_and_observes_revocation -- --exact --nocapture
# Use its printed source/revision, linked file pinned/original.txt and source-check original.txt.
# The separate imported_foreign_repository_pin_reads_from_independent_stores_and_rechecks_access
# test covers a foreign repository partition and live authorization revocation.
set -euo pipefail
[[ $(uname -s) == Linux ]]
driver_lore=$(realpath "${1:?lore executable required}")
driver_source=$(realpath "${2:?normal checkout required}")
driver_pin=${3:?exact root signature required}
driver_link=${4:?relative linked file required}
driver_check=${5:-original.txt}
[[ "$driver_pin" =~ ^[[:xdigit:]]{64}$ ]]
[[ "$driver_link" != /* && "$driver_link" != *..* ]]
driver_root=$(mktemp -d "${TMPDIR:-/tmp}/lore-working-linked.XXXXXX")
mkdir -m 700 "$driver_root/global" "$driver_root/auth" "$driver_root/runtime" "$driver_root/metadata" "$driver_root/mount"
export LORE_GLOBAL_PATH="$driver_root/global" LORE_AUTH_PATH="$driver_root/auth" XDG_RUNTIME_DIR="$driver_root/runtime"
export LORE_SERVICE_SOCKET=working-linked-smoke LORE_SERVICE_EXECUTABLE="$driver_lore" LORE_USE_SERVICE=1
cleanup() { timeout 40 "$driver_lore" --offline service stop >/dev/null 2>&1 || true; }
trap cleanup EXIT
lore() { timeout 90 "$driver_lore" --offline "$@"; }
sha256sum "$driver_source/$driver_check" "$driver_source/.lore/id" "$driver_source/.lore/instance" "$driver_source/.lore/config.toml" > "$driver_root/source-before.sha256"
set -x
lore mount --experimental --writable --source "$driver_source" --revision "$driver_pin" --metadata "$driver_root/metadata" --backing "$driver_root/backing" --digest-cache "$driver_root/cache" "$driver_root/mount"
cmp "$driver_source/$driver_check" "$driver_root/mount/$driver_link"
python3 - "$driver_root/mount/$driver_link" <<'PY'
import hashlib, os, sys
with open(sys.argv[1], "rb") as file:
    expected = hashlib.sha256(file.read()).digest()
assert os.getxattr(sys.argv[1], "user.sha256") == expected
PY
cat > "$driver_root/read.cpp" <<'CPP'
#include <fstream>
#include <iostream>
int main(int argc, char** argv) { if (argc != 2) return 2; std::ifstream file(argv[1]); if (!file) return 3; std::cout << file.rdbuf(); }
CPP
c++ "$driver_root/read.cpp" -o "$driver_root/read-linked"
test "$("$driver_root/read-linked" "$driver_root/mount/$driver_link")" = "$(cat "$driver_source/$driver_check")"
python3 - "$driver_root/mount/$driver_link" <<'PY'
import errno, os, sys
with open(sys.argv[1], "wb") as file:
    file.write(b"linked private bytes")
    file.flush()
    os.fsync(file.fileno())
try:
    os.getxattr(sys.argv[1], "user.sha256")
except OSError as error:
    assert error.errno == errno.ENODATA, error
else:
    raise AssertionError("dirty linked bytes exposed an immutable digest")
PY
sha256sum -c "$driver_root/source-before.sha256"
lore service stop
if mountpoint -q "$driver_root/mount"; then exit 1; fi
lore mount --experimental --writable --reuse --source "$driver_source" --metadata "$driver_root/metadata" --digest-cache "$driver_root/cache" "$driver_root/mount"
test "$("$driver_root/read-linked" "$driver_root/mount/$driver_link")" = 'linked private bytes'
sha256sum -c "$driver_root/source-before.sha256"
lore service stop
if mountpoint -q "$driver_root/mount"; then exit 1; fi
printf 'LINUX_WORKING_LINKED_PASS %s\n' "$driver_root"
