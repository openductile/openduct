# Copyright 2026 Ductile AI, Inc
# SPDX-License-Identifier: Apache-2.0
#
# OpenDuct on one virtual machine on Google Cloud (Compute Engine), as OpenTofu sets it up (../README.md): Debian 12,
# the released program as a systemd service (installed by the machine's startup script, which runs at every boot
# and changes nothing once the release is in place), its data on the machine's disk. The studio's port is open
# only to the addresses given (none by default), Kafka's to none.

terraform {
  required_version = ">= 1.12.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.0"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
}

locals {
  arm    = var.arch == "arm64"
  target = local.arm ? "aarch64-unknown-linux-gnu" : "x86_64-unknown-linux-gnu"
  tag    = "${var.name}-openduct"
}

resource "google_compute_firewall" "studio" {
  count         = length(var.studio_cidrs) > 0 ? 1 : 0
  name          = "${var.name}-studio"
  network       = var.network
  description   = "OpenDuct: the studio and webhooks"
  source_ranges = var.studio_cidrs
  target_tags   = [local.tag]
  allow {
    protocol = "tcp"
    ports    = ["5480"]
  }
}

resource "google_compute_firewall" "ssh" {
  count         = length(var.ssh_cidrs) > 0 ? 1 : 0
  name          = "${var.name}-ssh"
  network       = var.network
  description   = "OpenDuct: SSH"
  source_ranges = var.ssh_cidrs
  target_tags   = [local.tag]
  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}

resource "google_compute_instance" "openduct" {
  name         = var.name
  zone         = var.zone
  machine_type = var.machine_type != "" ? var.machine_type : (local.arm ? "t2a-standard-2" : "e2-standard-2")
  tags         = [local.tag]
  labels       = merge({ "part-of" = "openduct" }, var.labels)

  boot_disk {
    initialize_params {
      image = local.arm ? "debian-cloud/debian-12-arm64" : "debian-cloud/debian-12"
      size  = var.disk_gb
      type  = "pd-balanced"
    }
  }

  network_interface {
    network = var.network
    dynamic "access_config" {
      for_each = var.public_ip ? [1] : []
      content {}
    }
  }

  shielded_instance_config {
    enable_secure_boot          = true
    enable_vtpm                 = true
    enable_integrity_monitoring = true
  }

  # The start script as metadata (changed in place, run at the next boot), never metadata_startup_script, whose
  # change would make the machine - and its disk, with the data - anew.
  metadata = {
    block-project-ssh-keys = tostring(var.block_project_ssh_keys)
    startup-script         = templatefile("${path.module}/../vm-install.sh.tftpl", {
      version        = var.openduct_version
      target         = local.target
      studio_hosts   = join(",", var.studio_hosts)
      secure_cookies = var.secure_cookies
    })
  }

  lifecycle {
    ignore_changes = [boot_disk[0].initialize_params[0].image]
  }
}
