#!/usr/bin/env bash
# Configure Google Cloud Ops Agent for lore-server. The agent binary itself
# is already installed in the proxybase image (its own install-base-deps.sh);
# this only writes the per-daemon config.yaml, mirroring nomad-server's
# install-nomad-ops-agent.sh.
#
# Journal-only for now, same call nomad-server made: lore-server's telemetry
# is OTLP-push based (see lore-server/src/telemetry/), not a Prometheus
# `/metrics` endpoint, so there's nothing local for Ops Agent to scrape.
# Metrics/traces should point at the org's OTel collector directly via
# [telemetry] config, independent of this file. Revisit if lore-server ever
# grows a Prometheus exporter.
set -euo pipefail

echo "=== Configuring Ops Agent for lore-server (journal only) ==="
sudo tee /etc/google-cloud-ops-agent/config.yaml > /dev/null <<'EOF'
logging:
  receivers:
    lore_journal:
      type: systemd_journald
  processors:
    filter_lore:
      type: exclude_logs
      match_any:
        - 'NOT (jsonPayload._SYSTEMD_UNIT = "lore.service" OR jsonPayload._SYSTEMD_UNIT = "lore-bootstrap.service" OR jsonPayload._SYSTEMD_UNIT = "lore-cache-storage.service")'
  service:
    pipelines:
      lore:
        receivers:
          - lore_journal
        processors:
          - filter_lore
EOF

echo "=== Ops Agent configured for lore-server ==="
