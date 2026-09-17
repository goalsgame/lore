#!/usr/bin/env bash
# Configure Google Cloud Ops Agent for lore-server. The agent binary itself
# is already installed in the proxybase image (its own install-base-deps.sh);
# this only writes the per-daemon config.yaml, mirroring nomad-server's
# install-nomad-ops-agent.sh.
#
# Journal logging, plus an OTLP receiver (localhost:4317) for lore-server's
# own metrics/traces (see lore-server/src/telemetry/) -- the OTLP receiver
# doesn't carry logs, so those stay on the journald pipeline. lore-server
# only exports over OTLP when its own [telemetry.exporter] config points at
# this receiver; see the terraform-gcp-modules lore module's enable_telemetry
# variable.
set -euo pipefail

echo "=== Configuring Ops Agent for lore-server ==="
sudo tee /etc/google-cloud-ops-agent/config.yaml > /dev/null <<'EOF'
combined:
  receivers:
    otlp:
      type: otlp
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
metrics:
  service:
    pipelines:
      otlp:
        receivers:
          - otlp
traces:
  service:
    pipelines:
      otlp:
        receivers:
          - otlp
EOF

echo "=== Ops Agent configured for lore-server ==="
