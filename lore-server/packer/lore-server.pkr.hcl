// lore-server.pkr.hcl — GCE image for GOALS' lore-server.
//
// Self-contained to this repo rather than added to the org's shared
// nonseastarq image pipeline (every other GCE-VM daemon in the fleet is
// built there): lore-server's release cadence and ownership belong with
// this repo, not with the team that owns proxy tooling. The one thing it
// does share with that fleet is the base image (see proxy_base_version
// below) — reusing it is a deliberate, low-cost convention already
// established by nomad-server, not a dependency on nonseastarq's workflow.
//
// amd64 only, on purpose: `.cargo/config.toml`'s
// `[target.aarch64-unknown-linux-gnu]` block pins
// `-C target-cpu=neoverse-512tvb`, tuned for AWS Graviton3. GCP's arm64
// offering (Axion/C4A, Neoverse V2) is a different microarchitecture, and
// nothing here has verified that binary is safe to run on it. Revisit once
// that's actually tested — building arm64 blind risks a SIGILL in
// production. x86_64-unknown-linux-gnu has no such pinning and runs on any
// standard GCP amd64 machine type.
//
// Durable state (content-addressed fragments, branch pointers, locks) lives
// in GCS/Firestore via the lore-gcp plugin, not on this VM — so unlike a
// typical stateful daemon image, there's no data disk to provision here.
// The boot disk only needs to hold the OS, the binary, and config.
//
// TLS: lore-server terminates its own TLS (required for QUIC) and there's
// no established org pattern for that on a bare VM yet. lore-bootstrap.sh
// (see scripts/) is where a fetched cert/key would land once that's
// decided — /etc/lore/certs exists and is owned by the `lore` user, but
// nothing populates it today. Config layering and the systemd unit are
// otherwise fully wired.

packer {
  required_plugins {
    googlecompute = {
      source  = "github.com/hashicorp/googlecompute"
      version = "~> 1.1"
    }
  }
}

variable "project_id" {
  type        = string
  description = "GCP project ID to build in"
}

variable "zone" {
  type        = string
  default     = "europe-west1-b"
  description = "GCP zone for the build VM"
}

variable "machine_type" {
  type        = string
  default     = "n2-standard-2"
  description = "Machine type for the build VM. No compilation happens here — the binary arrives prebuilt — so this only needs to be big enough to copy files and run a couple of shell provisioners."
}

variable "image_name" {
  type        = string
  default     = ""
  description = "Output image name (default: lore-server-amd64-v{version})"
}

variable "image_family" {
  type        = string
  default     = "lore-server-amd64"
  description = "Image family for the output image"
}

variable "image_project_id" {
  type        = string
  default     = ""
  description = "GCP project to store the output image in (default: same as project_id)"
}

variable "service_account_email" {
  type        = string
  default     = ""
  description = "Service account for the build VM"
}

variable "network_project_id" {
  type        = string
  default     = ""
  description = "Shared VPC host project (default: same as project_id)"
}

variable "network" {
  type        = string
  default     = "default"
  description = "VPC network for the build VM"
}

variable "subnetwork" {
  type        = string
  default     = ""
  description = "Subnetwork for the build VM (required for shared VPCs)"
}

variable "version" {
  type        = string
  description = "lore-server release version being baked (e.g. a git tag like v0.9.2, or an X.Y.Z). Used only for the output image name — the binary itself is whatever release-binaries/loreserver the caller staged. Sanitized for GCE's image-name character set below; no need to strip a leading \"v\" or dots yourself."
}

variable "proxy_base_version" {
  type        = string
  default     = "2"
  description = "Version of the org's shared proxybase-{version}-amd64 base image to build on. lore-server only needs what that image already provides — Debian 13, node_exporter, journal access, gcloud/gsutil — so it reuses it rather than standing up a separate base, the same call nomad-server made."
}

variable "proxy_base_image_project_id" {
  type        = string
  default     = ""
  description = "GCP project the proxybase image lives in (default: same as image_project_id, i.e. the artifacts project)"
}

locals {
  // GCE image names must be lowercase and may only contain letters, digits,
  // and dashes — no dots. Mirrors nomad-server.pkr.hcl's version_suffix
  // exactly (plain literal replace, not a regex: a first attempt here used
  // replace()'s Terraform-style "/regex/" form, which Packer's HCL does not
  // support the same way — it matched nothing and passed the dots straight
  // through, caught by a failed local validation build).
  version_sanitized       = lower(replace(var.version, ".", "-"))
  image_name              = var.image_name != "" ? var.image_name : "lore-server-amd64-v${local.version_sanitized}"
  image_project_id        = var.image_project_id != "" ? var.image_project_id : var.project_id
  source_image            = "proxybase-${var.proxy_base_version}-amd64"
  source_image_project_id = var.proxy_base_image_project_id != "" ? var.proxy_base_image_project_id : local.image_project_id
}

source "googlecompute" "lore_server" {
  project_id = var.project_id

  zone         = var.zone
  machine_type = var.machine_type

  // Pinned to a specific proxybase image, not the family, so a base rebuild
  // never changes what lore-server ships on without an explicit version bump
  // here.
  source_image            = local.source_image
  source_image_project_id = [local.source_image_project_id]

  image_name        = local.image_name
  image_family      = var.image_family
  image_project_id  = local.image_project_id
  image_description = "lore-server (amd64) v${var.version} on proxybase-${var.proxy_base_version}"

  disk_size = 20
  disk_type = "pd-ssd"

  service_account_email = var.service_account_email != "" ? var.service_account_email : null

  network_project_id = var.network_project_id != "" ? var.network_project_id : null
  network            = var.network
  subnetwork         = var.subnetwork != "" ? var.subnetwork : null
  omit_external_ip   = true
  use_internal_ip    = true
  use_iap            = true
  tags               = ["egress-inet"]

  ssh_username = "packer"
}

build {
  sources = ["source.googlecompute.lore_server"]

  # Sanity-check the base image. Fails loud and early rather than baking a
  # broken image if someone points this at the wrong proxybase version.
  provisioner "shell" {
    inline = [
      "command -v gcloud >/dev/null || (echo 'FAIL: gcloud missing — needed by lore-bootstrap to fetch Secret Manager values'; exit 1)",
      "echo 'proxy-base sanity check OK'",
    ]
  }

  # Stage the release binary. Built by the calling workflow (see
  # .github/workflows/packer-images.yml) into release-binaries/loreserver
  # before packer runs — nothing is compiled on this VM.
  #
  # The mkdir here isn't redundant: with exactly one file in
  # release-binaries/, Packer's file provisioner (SCP under the hood) is
  # ambiguous about whether a non-existent destination should become a
  # directory or the uploaded file itself renamed to that path — confirmed
  # by a local validation build that landed the binary at
  # /tmp/lore-server-release (a file) instead of
  # /tmp/lore-server-release/loreserver, failing install-lore.sh with "Not
  # a directory". Pre-creating the destination as a real directory removes
  # the ambiguity.
  provisioner "shell" {
    inline = ["mkdir -p /tmp/lore-server-release"]
  }

  provisioner "file" {
    source      = "release-binaries/"
    destination = "/tmp/lore-server-release"
  }

  # Stage systemd units and the bootstrap/cache-storage scripts.
  provisioner "file" {
    source      = "systemd/lore.service"
    destination = "/tmp/lore.service"
  }

  provisioner "file" {
    source      = "systemd/lore-bootstrap.service"
    destination = "/tmp/lore-bootstrap.service"
  }

  provisioner "file" {
    source      = "scripts/lore-bootstrap.sh"
    destination = "/tmp/lore-bootstrap.sh"
  }

  provisioner "file" {
    source      = "systemd/lore-cache-storage.service"
    destination = "/tmp/lore-cache-storage.service"
  }

  provisioner "file" {
    source      = "scripts/lore-cache-storage-init.sh"
    destination = "/tmp/lore-cache-storage-init.sh"
  }

  # Install everything. Deliberately does NOT `systemctl start lore` —
  # startup depends on per-environment config (LORE_ENV, secrets) that only
  # exists once a real instance boots from Terraform-supplied metadata, not
  # during the image bake. `enable` is safe here; `start` is not.
  provisioner "shell" {
    script = "scripts/install-lore.sh"
  }

  # Ops Agent binary already ships in proxy-base; this just points it at
  # lore-server's journal (matching nomad-server's journal-only setup — see
  # scripts/install-lore-ops-agent.sh for why metrics scraping isn't wired
  # up here). Journal access for non-root SSH users (pam_group ->
  # systemd-journal) is also already configured by proxy-base — nothing
  # lore-specific needed there.
  provisioner "shell" {
    script = "scripts/install-lore-ops-agent.sh"
  }

  provisioner "shell" {
    inline = [
      "sudo apt-get clean",
      "sudo rm -rf /var/lib/apt/lists/*",
    ]
  }
}
