# Copyright 2026 Ductile AI, Inc
# SPDX-License-Identifier: Apache-2.0

output "studio" {
  description = "Where the studio is."
  value       = "http://localhost:${var.studio_port}"
}

output "kafka" {
  description = "Where Kafka clients connect."
  value       = "localhost:${var.kafka_port}"
}

output "postgres_credential" {
  description = "What a workflow's postgres credential holds, for the database beside OpenDuct (tofu output -json postgres_credential)."
  sensitive   = true
  value = {
    url      = "postgres://postgres:5432/openduct"
    username = "openduct"
    password = random_password.postgres_password.result
    tls      = "disable"
  }
}

output "s3" {
  description = "The S3 store's address inside the network, and its keys (tofu output -json s3)."
  sensitive   = true
  value = {
    endpoint   = "http://s3:8333"
    access_key = var.s3_access_key
    secret_key = random_password.s3_secret_key.result
  }
}
