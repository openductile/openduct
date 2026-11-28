# Copyright 2026 Ductile AI, Inc
# SPDX-License-Identifier: Apache-2.0

variable "project" {
  description = "The Google Cloud project."
  type        = string
}

variable "name" {
  description = "The name of the machine and of what is made with it (small letters, digits, dashes)."
  type        = string
  default     = "openduct"
}

variable "region" {
  description = "The region."
  type        = string
  default     = "europe-west3"
}

variable "zone" {
  description = "The zone of the machine."
  type        = string
  default     = "europe-west3-a"
}

variable "openduct_version" {
  description = "The OpenDuct release to run, as v0.1.2."
  type        = string
  default     = "v0.1.2"
}

variable "arch" {
  description = "amd64 or arm64."
  type        = string
  default     = "amd64"
  validation {
    condition     = contains(["amd64", "arm64"], var.arch)
    error_message = "arch: amd64 or arm64."
  }
}

variable "machine_type" {
  description = "The machine type; empty for e2-standard-2 (amd64) or t2a-standard-2 (arm64)."
  type        = string
  default     = ""
}

variable "disk_gb" {
  description = "The disk's size, in GB: the data lives on it."
  type        = number
  default     = 40
}

variable "network" {
  description = "The VPC network."
  type        = string
  default     = "default"
}

variable "public_ip" {
  description = "Give the machine a public address."
  type        = bool
  default     = true
}

variable "block_project_ssh_keys" {
  description = "Refuse the project's SSH keys on this machine (sign in with OS Login or the machine's own keys)."
  type        = bool
  default     = true
}

variable "studio_hosts" {
  description = "The names the studio is reached by (beside localhost)."
  type        = list(string)
  default     = []
}

variable "secure_cookies" {
  description = "Mark the studio's cookies Secure: true once a TLS proxy is in front of it."
  type        = bool
  default     = false
}

variable "studio_cidrs" {
  description = "Addresses that may reach the studio's port 5480 directly; none by default."
  type        = list(string)
  default     = []
}

variable "ssh_cidrs" {
  description = "Addresses that may reach SSH; none by default."
  type        = list(string)
  default     = []
}

variable "labels" {
  description = "More labels for the machine."
  type        = map(string)
  default     = {}
}
