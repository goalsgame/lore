#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Epic Games, Inc.
# SPDX-License-Identifier: MIT
"""A fake Google Compute Engine metadata server, for local/CI testing of `lore-gcp` against the
Firestore emulator and the Google Cloud Storage testbench only. This is never used in production
and never talks to a real Google service.

# Why this exists

Both `firestore` (via `gcloud-sdk`) and `google-cloud-storage` (via `google-cloud-auth`) resolve
Application Default Credentials the same way real GCP client libraries do, and neither treats an
emulator/test-double endpoint as a reason to skip authentication: a store built against
`FIRESTORE_EMULATOR_HOST` or a `LORE_GCP_TEST_GCS_ENDPOINT` override still tries to mint a real
bearer token first. Two ways of supplying that were tried and rejected before this one:

- A fake `GOOGLE_APPLICATION_CREDENTIALS` service-account-key file. `google-cloud-auth` accepts
  this fine (it mints and sends a self-signed JWT directly as the bearer token, which the
  testbench does not verify). But `gcloud-sdk` 0.28.5 (the version `firestore = "=0.47.1"` pulls
  in, chosen because newer releases require the `aws-lc-rs` backend this workspace's `deny.toml`
  forbids) does not: its service-account credential type performs a real OAuth2 JWT-bearer grant
  exchange against `https://oauth2.googleapis.com/token`, which rejects a JWT signed by a key
  Google never issued, with an opaque 400. That is a real network round trip this crate's test
  suite must not depend on, and it cannot be fixed without either bumping past the `deny.toml`
  boundary or patching `gcloud-sdk`.
- No credentials at all. Both libraries then fail locally before ever reaching the network,
  since `find_default`/ADC resolution requires *something* to succeed.

What both libraries share is a metadata-service (MDS) credential path: when nothing else is
configured, both fall back to querying `GCE_METADATA_HOST` (default `metadata.google.internal`)
for a bearer token over plain HTTP, exactly as code running on a real GCE/GKE/Cloud Run instance
does. This server implements just enough of that HTTP surface to satisfy both libraries entirely
locally, with a token neither the emulator nor the testbench ever cryptographically verifies:

- `firestore`/`gcloud-sdk` accepts any token `google-cloud-auth` would (the shapes below are a
  superset, not filtered by consumer).
- The Firestore emulator additionally insists the bearer token be *shaped* like a JWT (three
  dot-separated, base64url-encoded segments) — but never verifies its signature — so the access
  token minted here is an unsigned, structurally valid JWT rather than the opaque string a real
  MDS token normally is.

# Usage

Run with the target project id as the sole argument, bind to port 80 (the port both client
libraries always use for MDS, regardless of the configured host), and point `GCE_METADATA_HOST`
at this server's host (bare, no port) — or give it the network alias `metadata.google.internal`
directly, which needs no environment variable at all since that is the MDS default host. See
`lore-integration-tests/compose.yaml`'s `gcp-fake-metadata` service and
`.github/workflows/pr-validate.yml`'s `gcp-integration` job for how this is wired up:
`GOOGLE_APPLICATION_CREDENTIALS` is deliberately left unset so ADC resolution falls through to
this server.
"""

import base64
import http.server
import json
import sys
import time

PROJECT_ID = sys.argv[1] if len(sys.argv) > 1 else "test-project"


def _b64url(obj: dict) -> str:
    return base64.urlsafe_b64encode(json.dumps(obj).encode()).rstrip(b"=").decode()


def _fake_unsigned_jwt() -> str:
    """A structurally valid (three dot-separated, base64url segments) but unsigned JWT. Real
    Firestore, even against its own local emulator, requires a JWT-shaped bearer token; the
    emulator does not verify the signature, so the empty third segment is accepted."""
    header = _b64url({"alg": "none", "typ": "JWT"})
    now = int(time.time())
    payload = _b64url(
        {
            "iss": "https://accounts.google.com",
            "sub": "lore-gcp-ci-fake-user",
            "email": f"lore-gcp-ci@{PROJECT_ID}.iam.gserviceaccount.com",
            "aud": PROJECT_ID,
            "iat": now,
            "exp": now + 3600,
        }
    )
    return f"{header}.{payload}."


class Handler(http.server.BaseHTTPRequestHandler):
    def _respond(self, body: bytes, content_type: str) -> None:
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        # Real GCE metadata responses carry this header; some client libraries check for it.
        self.send_header("Metadata-Flavor", "Google")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json(self, obj: dict) -> None:
        self._respond(json.dumps(obj).encode(), "application/json")

    def _text(self, text: str) -> None:
        self._respond(text.encode(), "text/plain")

    def do_GET(self) -> None:  # noqa: N802 (BaseHTTPRequestHandler's naming convention)
        path = self.path.split("?", 1)[0]
        if path == "/":
            # Real MDS instances answer the bare root; gcloud-sdk's availability probe uses this.
            self._text("Metadata-Flavor: Google")
        elif path == "/computeMetadata/v1/instance/service-accounts/default/token":
            self._json(
                {
                    "access_token": _fake_unsigned_jwt(),
                    "expires_in": 3600,
                    "token_type": "Bearer",
                }
            )
        elif path == "/computeMetadata/v1/instance/service-accounts/default/email":
            self._text(f"lore-gcp-ci@{PROJECT_ID}.iam.gserviceaccount.com")
        elif path == "/computeMetadata/v1/project/project-id":
            self._text(PROJECT_ID)
        elif path == "/computeMetadata/v1/universe/universe-domain":
            self._text("googleapis.com")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, fmt: str, *args: object) -> None:
        sys.stderr.write("gcp-fake-metadata: " + (fmt % args) + "\n")


if __name__ == "__main__":
    http.server.HTTPServer(("0.0.0.0", 80), Handler).serve_forever()
