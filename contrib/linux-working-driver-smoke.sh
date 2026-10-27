#!/usr/bin/env bash
# Exercise the Linux driver with a real populated checkout and an isolated service.
# Usage: contrib/linux-working-driver-smoke.sh /absolute/path/to/lore
# Requires /dev/fuse, a mount helper, Python 3, GNU timeout and a C++ compiler.
# The fixture is retained at the printed path; HOME and other services are untouched.
set -euo pipefail
[[ $(uname -s) == Linux ]] || { echo 'This acceptance test requires Linux.' >&2; exit 1; }
driver_lore=$(realpath "${1:?usage: $0 /path/to/lore}")
[[ -x "$driver_lore" && -c /dev/fuse ]]
driver_root=$(mktemp -d "${TMPDIR:-/tmp}/lore-working-driver.XXXXXX")
mkdir -m 700 "$driver_root/global" "$driver_root/auth" "$driver_root/runtime" "$driver_root/source" "$driver_root/metadata" "$driver_root/mount"
export LORE_GLOBAL_PATH="$driver_root/global" LORE_AUTH_PATH="$driver_root/auth" XDG_RUNTIME_DIR="$driver_root/runtime"
export LORE_SERVICE_SOCKET=working-driver-smoke LORE_SERVICE_EXECUTABLE="$driver_lore" LORE_USE_SERVICE=1
cleanup() { timeout 40 "$driver_lore" --offline service stop >/dev/null 2>&1 || true; }
trap cleanup EXIT
lore() { timeout 90 "$driver_lore" --offline "$@"; }
hash_xattr() {
    python3 - "$driver_root/mount/value.h" "$1" <<'PY'
import errno, hashlib, os, sys
path, mode = sys.argv[1:]
if mode == "absent":
    try:
        os.getxattr(path, "user.sha256")
    except OSError as error:
        assert error.errno == errno.ENODATA, error
    else:
        raise AssertionError("private edits exposed an immutable hash")
else:
    with open(path, "rb") as file:
        expected = hashlib.sha256(file.read()).digest()
    actual = os.getxattr(path, "user.sha256")
    assert len(actual) == 32 and actual == expected, (actual.hex(), expected.hex())
PY
}
set -x
lore --no-gc --repository "$driver_root/source" repository create working-driver-smoke
cat > "$driver_root/source/main.cpp" <<'CPP'
#include <cstdio>
#include "value.h"
int main() { std::printf("%d\n", value()); }
CPP
cat > "$driver_root/source/value.h" <<'CPP'
inline int value() { return 42; }
CPP
lore --repository "$driver_root/source" stage --scan "$driver_root/source/main.cpp" "$driver_root/source/value.h"
lore --repository "$driver_root/source" commit 'populated driver source'
sha256sum "$driver_root/source/main.cpp" "$driver_root/source/value.h" "$driver_root/source/.lore/id" "$driver_root/source/.lore/instance" "$driver_root/source/.lore/config.toml" > "$driver_root/source-before.sha256"
lore mount --experimental --writable --source "$driver_root/source" --revision main@LATEST --metadata "$driver_root/metadata" --backing "$driver_root/backing" --digest-cache "$driver_root/digest-cache" "$driver_root/mount"
mountpoint -q "$driver_root/mount"
hash_xattr present
c++ "$driver_root/mount/main.cpp" -I "$driver_root/mount" -o "$driver_root/program-before"
test "$("$driver_root/program-before")" = 42
python3 - "$driver_root/mount/value.h" <<'PY'
import os, sys
with open(sys.argv[1], "wb") as file:
    file.write(b"inline int value() { return 43; }\n")
    file.flush()
    os.fsync(file.fileno())
PY
hash_xattr absent
c++ "$driver_root/mount/main.cpp" -I "$driver_root/mount" -o "$driver_root/program-after"
test "$("$driver_root/program-after")" = 43
lore --repository "$driver_root/mount" status --scan
lore --repository "$driver_root/mount" stage --scan "$driver_root/mount/value.h"
lore --repository "$driver_root/mount" commit 'working driver local commit'
hash_xattr present
lore --repository "$driver_root/mount" status --scan
sha256sum -c "$driver_root/source-before.sha256"
lore service stop
if mountpoint -q "$driver_root/mount"; then exit 1; fi
lore mount --experimental --writable --reuse --source "$driver_root/source" --metadata "$driver_root/metadata" --digest-cache "$driver_root/digest-cache" "$driver_root/mount"
hash_xattr present
c++ "$driver_root/mount/main.cpp" -I "$driver_root/mount" -o "$driver_root/program-reused"
test "$("$driver_root/program-reused")" = 43
sha256sum -c "$driver_root/source-before.sha256"
lore service stop
if mountpoint -q "$driver_root/mount"; then exit 1; fi
printf 'LINUX_WORKING_DRIVER_PASS %s\n' "$driver_root"
