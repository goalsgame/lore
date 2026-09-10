#!/usr/bin/env bash
# Runs once per boot, before lore.service, as root (see
# ../systemd/lore-bootstrap.service). Pulls everything that varies per
# environment out of instance metadata and Secret Manager, so one image can
# power dev/staging/prod — the same shape nomad-bootstrap.sh uses for Nomad.
#
# Metadata keys read (all optional; a missing key is treated as "not
# configured yet", not an error, so this never blocks boot):
#
#   lore-environment   Plain value written as LORE_ENV=<value>.
#   lore-config-toml   Full TOML text, written verbatim as local.toml — the
#                       config directory's last, highest-priority file layer
#                       (see docs/reference/lore-server-config.md). For
#                       non-secret config: plugin selection, bucket/project
#                       names, jwt_issuer/jwt_audience, permission_claim.
#   lore-acl-toml      Full TOML text, written verbatim as acl.toml — the
#                       file server.auth.acl_config_path (set in local.toml,
#                       above) points at, for ConfiguredGrantsAuthorizer. See
#                       lore-server/src/authnz/acl_config.rs.
#   lore-secret-refs   One `ENV_VAR_NAME=secret-ref` pair per line, where
#                       secret-ref is a fully-qualified Secret Manager
#                       resource name (e.g.
#                       projects/123/secrets/scrt-prod-euw1-lore-x/versions/latest),
#                       matching the org's convention of passing secrets by
#                       reference rather than by value. Each is resolved via
#                       `gcloud secrets versions access` and written to
#                       secrets.env as ENV_VAR_NAME=<value>. Any LORE__-prefixed
#                       name here overrides the matching field in local.toml
#                       (environment variables win over every file layer).
#   lore-cert-secret-refs  Same secret-ref convention as above, but for
#                       secrets that must land as files rather than
#                       environment variables (TLS certificate/key/chain
#                       PEMs: local.toml's [server.*.certificate] blocks take
#                       file paths, not inline values). One
#                       `filename=secret-ref` pair per line; each is resolved
#                       and written to config/tls/<filename>.
set -euo pipefail

CONFIG_DIR=/etc/lore/config
METADATA_BASE="http://metadata.google.internal/computeMetadata/v1/instance/attributes"

fetch_metadata() {
  local key="$1"
  curl -fsS -H "Metadata-Flavor: Google" "${METADATA_BASE}/${key}" 2>/dev/null || true
}

# `gcloud secrets versions access` takes the version and the bare secret name
# as separate arguments -- passing the fully-qualified
# projects/<p>/secrets/<s>/versions/<v> path as --secret 404s, it does not
# parse that form itself. Splits one before calling it.
resolve_secret_ref() {
  local ref="$1"
  if [[ "$ref" =~ ^projects/([^/]+)/secrets/([^/]+)/versions/([^/]+)$ ]]; then
    gcloud secrets versions access "${BASH_REMATCH[3]}" \
      --secret="${BASH_REMATCH[2]}" \
      --project="${BASH_REMATCH[1]}" 2>/dev/null || true
  fi
}

install -d -m 0750 -o lore -g lore "$CONFIG_DIR"

# LORE_ENV
environment="$(fetch_metadata lore-environment)"
{
  if [[ -n "$environment" ]]; then
    printf 'LORE_ENV=%s\n' "$environment"
  fi
} >"$CONFIG_DIR/lore-env.env"
chown lore:lore "$CONFIG_DIR/lore-env.env"
chmod 0640 "$CONFIG_DIR/lore-env.env"

# local.toml — non-secret, per-environment config.
config_toml="$(fetch_metadata lore-config-toml)"
if [[ -n "$config_toml" ]]; then
  printf '%s\n' "$config_toml" >"$CONFIG_DIR/local.toml"
  chown lore:lore "$CONFIG_DIR/local.toml"
  chmod 0640 "$CONFIG_DIR/local.toml"
fi

# acl.toml — non-secret, per-environment ConfiguredGrantsAuthorizer config.
acl_toml="$(fetch_metadata lore-acl-toml)"
if [[ -n "$acl_toml" ]]; then
  printf '%s\n' "$acl_toml" >"$CONFIG_DIR/acl.toml"
  chown lore:lore "$CONFIG_DIR/acl.toml"
  chmod 0640 "$CONFIG_DIR/acl.toml"
fi

# secrets.env — resolved from Secret Manager, never written to metadata or
# local.toml. World-unreadable; only root and the lore group can read it.
secrets_file="$CONFIG_DIR/secrets.env"
: >"$secrets_file"
chown lore:lore "$secrets_file"
chmod 0600 "$secrets_file"

secret_refs="$(fetch_metadata lore-secret-refs)"
if [[ -n "$secret_refs" ]]; then
  while IFS='=' read -r name ref; do
    [[ -z "$name" || -z "$ref" ]] && continue
    value="$(resolve_secret_ref "$ref")"
    if [[ -z "$value" ]]; then
      echo "lore-bootstrap: WARNING: could not resolve secret ref for ${name} (${ref}), skipping" >&2
      continue
    fi
    printf '%s=%s\n' "$name" "$value" >>"$secrets_file"
  done <<<"$secret_refs"
fi

# config/tls/<filename> — resolved from Secret Manager, same as secrets.env,
# but as files: TLS certificate/key/chain PEMs are referenced by path from
# local.toml, not inlined.
cert_secret_refs="$(fetch_metadata lore-cert-secret-refs)"
if [[ -n "$cert_secret_refs" ]]; then
  tls_dir="$CONFIG_DIR/tls"
  install -d -m 0750 -o lore -g lore "$tls_dir"
  while IFS='=' read -r filename ref; do
    [[ -z "$filename" || -z "$ref" ]] && continue
    value="$(resolve_secret_ref "$ref")"
    if [[ -z "$value" ]]; then
      echo "lore-bootstrap: WARNING: could not resolve cert secret ref for ${filename} (${ref}), skipping" >&2
      continue
    fi
    printf '%s' "$value" >"$tls_dir/$filename"
    chown lore:lore "$tls_dir/$filename"
    chmod 0640 "$tls_dir/$filename"
  done <<<"$cert_secret_refs"
fi

echo "lore-bootstrap: done (environment=${environment:-<unset>})"
