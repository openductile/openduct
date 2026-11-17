# Copyright 2026 Ductile AI, Inc
# SPDX-License-Identifier: Apache-2.0

variable "name" {
  description = "What the containers, volumes and network are named after."
  type        = string
  default     = "openduct"
}

variable "openduct_image" {
  description = "OpenDuct's image: built from this repository by default, or one from your registry (set build_openduct = false)."
  type        = string
  default     = "openduct:local"
}

variable "build_openduct" {
  description = "Build OpenDuct's image from this repository."
  type        = bool
  default     = true
}

variable "s3_image" {
  description = "The S3 store's image (SeaweedFS)."
  type        = string
  default     = "chrislusf/seaweedfs:4.51"
}

variable "postgres_image" {
  description = "Postgres with pgvector."
  type        = string
  default     = "pgvector/pgvector:0.8.6-pg18"
}

variable "s3_access_key" {
  description = "The S3 store's access key (its secret key is made at random)."
  type        = string
  default     = "openduct"
}

variable "studio_port" {
  description = "The port on this machine the studio listens on."
  type        = number
  default     = 5480
}

variable "kafka_port" {
  description = "The port on this machine Kafka clients connect to."
  type        = number
  default     = 9092
}
