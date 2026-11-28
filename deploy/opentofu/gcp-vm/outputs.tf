# Copyright 2026 Ductile AI, Inc
# SPDX-License-Identifier: Apache-2.0

locals {
  address = try(google_compute_instance.openduct.network_interface[0].access_config[0].nat_ip, "")
}

output "instance" {
  description = "The machine."
  value       = google_compute_instance.openduct.self_link
}

output "public_ip" {
  description = "Its public address, if it has one."
  value       = local.address
}

output "studio" {
  description = "How to reach the studio."
  value = length(var.studio_cidrs) > 0 ? "http://${local.address}:5480" : (
    "gcloud compute ssh ${var.name} --zone ${var.zone} -- -L 5480:127.0.0.1:5480, then http://localhost:5480"
  )
}
