#!/usr/bin/env bash
# Runs once per boot (before lore-bootstrap.service) and on a daily timer
# (see ../systemd/lore-acme.{service,timer}), as root. Obtains/renews
# lore-server's real TLS certificate via Let's Encrypt (ACME HTTP-01, using
# `lego`) and, on an actual (re)issuance, restarts lore.service so it picks
# up the new cert -- lore-server has no hot-reload for TLS material.
#
# Metadata keys read (all optional; a missing/false lore-acme-enabled means
# this script is a no-op -- the same baked image also serves deployments
# using generate_self_signed_tls or a bring-your-own cert):
#
#   lore-acme-enabled          "true" to enable this script; anything else
#                               (including unset) is a no-op.
#   lore-acme-email            ACME account contact email.
#   lore-acme-domain           The single hostname to request a cert for
#                               (also the HTTP-01 challenge target).
#   lore-acme-server           Optional ACME directory URL override (e.g.
#                               Let's Encrypt's staging directory, for
#                               testing without hitting production rate
#                               limits or issuing a publicly-untrusted cert).
#   lore-acme-cert-secret-ref  Secret Manager ref (same fully-qualified-path
#                               convention as lore-bootstrap.sh's
#                               lore-cert-secret-refs) this script reads the
#                               currently-live cert from, and writes new
#                               versions to, as the fullchain PEM.
#   lore-acme-pkey-secret-ref  Same, for the private key PEM.
#
# Secret Manager, not lego's own local --path bookkeeping, is the source of
# truth for "is a renewal due": the MIG this runs on replaces the boot disk
# on every image bump (replacement_method = RECREATE), which would wipe any
# local ACME state and make a naive `lego renew` look like day-one issuance
# on every routine image bump -- risking Let's Encrypt's per-domain rate
# limits, given how often that has happened this session. This script always
# calls `lego run` (never `lego renew`), gated by its own due-check against
# the cert already sitting in Secret Manager.
set -euo pipefail

METADATA_BASE="http://metadata.google.internal/computeMetadata/v1/instance/attributes"
LEGO_PATH=/run/lore-acme
RENEW_THRESHOLD_SECONDS=$((30 * 86400)) # renew once fewer than 30 days remain

fetch_metadata() {
  local key="$1"
  curl -fsS -H "Metadata-Flavor: Google" "${METADATA_BASE}/${key}" 2>/dev/null || true
}

# Same as lore-bootstrap.sh's resolve_secret_ref -- duplicated rather than
# shared, to keep this script's diff/review surface isolated.
resolve_secret_ref() {
  local ref="$1"
  if [[ "$ref" =~ ^projects/([^/]+)/secrets/([^/]+)/versions/([^/]+)$ ]]; then
    gcloud secrets versions access "${BASH_REMATCH[3]}" \
      --secret="${BASH_REMATCH[2]}" \
      --project="${BASH_REMATCH[1]}" 2>/dev/null || true
  fi
}

secret_project_and_id() {
  local ref="$1"
  if [[ "$ref" =~ ^projects/([^/]+)/secrets/([^/]+)/versions/([^/]+)$ ]]; then
    printf '%s %s\n' "${BASH_REMATCH[1]}" "${BASH_REMATCH[2]}"
  fi
}

enabled="$(fetch_metadata lore-acme-enabled)"
if [[ "$enabled" != "true" ]]; then
  echo "lore-cert-renew: lore-acme-enabled is not \"true\", nothing to do"
  exit 0
fi

email="$(fetch_metadata lore-acme-email)"
domain="$(fetch_metadata lore-acme-domain)"
server="$(fetch_metadata lore-acme-server)"
cert_ref="$(fetch_metadata lore-acme-cert-secret-ref)"
pkey_ref="$(fetch_metadata lore-acme-pkey-secret-ref)"

for name_value in "email=$email" "domain=$domain" "cert_ref=$cert_ref" "pkey_ref=$pkey_ref"; do
  if [[ "${name_value#*=}" == "" ]]; then
    echo "lore-cert-renew: lore-acme-enabled is true but ${name_value%%=*} metadata is missing, aborting" >&2
    exit 1
  fi
done

if [[ "${LORE_ACME_FORCE:-}" != "1" ]]; then
  current_cert="$(resolve_secret_ref "$cert_ref")"
  if [[ -n "$current_cert" ]] && printf '%s' "$current_cert" | openssl x509 -checkend "$RENEW_THRESHOLD_SECONDS" -noout >/dev/null 2>&1; then
    echo "lore-cert-renew: current certificate for ${domain} is not due for renewal, skipping"
    exit 0
  fi
fi

echo "lore-cert-renew: obtaining/renewing certificate for ${domain}"

rm -rf "$LEGO_PATH"
install -d -m 0700 "$LEGO_PATH"

lego_args=(
  run
  --domains "$domain"
  --email "$email"
  --path "$LEGO_PATH"
  --http
  --accept-tos
)
[[ -n "$server" ]] && lego_args+=(--server "$server")

if ! lego "${lego_args[@]}"; then
  echo "lore-cert-renew: lego failed to obtain/renew a certificate for ${domain}" >&2
  rm -rf "$LEGO_PATH"
  exit 1
fi

cert_file="$LEGO_PATH/certificates/${domain}.crt"
pkey_file="$LEGO_PATH/certificates/${domain}.key"
if [[ ! -s "$cert_file" || ! -s "$pkey_file" ]]; then
  echo "lore-cert-renew: lego reported success but ${cert_file}/${pkey_file} are missing or empty" >&2
  rm -rf "$LEGO_PATH"
  exit 1
fi

read -r cert_project cert_secret <<<"$(secret_project_and_id "$cert_ref")"
read -r pkey_project pkey_secret <<<"$(secret_project_and_id "$pkey_ref")"

gcloud secrets versions add "$cert_secret" --project="$cert_project" --data-file="$cert_file"
gcloud secrets versions add "$pkey_secret" --project="$pkey_project" --data-file="$pkey_file"

# Also land the fresh material at the same local paths lore-bootstrap.sh
# writes them to (same ownership/permissions), unconditionally -- not just
# on first boot. lore-bootstrap.service only ever runs once per boot, so a
# later, timer-triggered renewal has nothing else that would refresh these
# files: without this, uploading a new Secret Manager version would do
# nothing locally, and a restart below would just reload the same stale
# on-disk cert. install -d is idempotent, safe whether or not
# lore-bootstrap.sh has run yet.
tls_dir=/etc/lore/config/tls
install -d -m 0750 -o lore -g lore /etc/lore/config
install -d -m 0750 -o lore -g lore "$tls_dir"
install -m 0640 -o lore -g lore "$cert_file" "$tls_dir/cert.pem"
install -m 0640 -o lore -g lore "$pkey_file" "$tls_dir/pkey.pem"

rm -rf "$LEGO_PATH"

# Only restart if lore.service has actually run before. lore.service
# Requires=lore-bootstrap.service (hard dependency), and lore-bootstrap.service
# is ordered After=lore-acme.service (this unit) -- so on first boot, while
# this script is still running as lore-acme.service's own foreground
# process, calling `systemctl restart lore.service` here would transitively
# try to start lore-bootstrap.service too, which can't start until
# lore-acme.service reaches a terminal state, which can't happen until the
# restart call returns: a genuine deadlock, confirmed live (systemctl
# restart hung until lore-acme.service's own TimeoutStartSec eventually
# killed it). On first boot, lore.service hasn't started even once yet, so
# no restart is needed at all -- lore-bootstrap.service runs next in the
# normal boot sequence and lore.service starts fresh, already reading the
# files just written above. This only actually restarts on a later,
# timer-triggered renewal, where lore.service is genuinely already active.
if systemctl is-active --quiet lore.service; then
  echo "lore-cert-renew: new certificate uploaded, restarting lore.service"
  systemctl restart lore.service
else
  echo "lore-cert-renew: new certificate uploaded; lore.service not active yet (first boot), skipping restart"
fi
