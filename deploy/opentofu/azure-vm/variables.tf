# Copyright 2026 Ductile AI, Inc
# SPDX-License-Identifier: Apache-2.0

variable "subscription_id" {
  description = "The Azure subscription."
  type        = string
}

variable "ssh_public_key" {
  description = "The public SSH key that signs in as admin_username (Azure's Linux machines need one)."
  type        = string
}

variable "name" {
  description = "The name of the machine and of what is made with it."
  type        = string
  default     = "openduct"
}

variable "location" {
  description = "The Azure region."
  type        = string
  default     = "germanywestcentral"
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

variable "size" {
  description = "The machine's size; empty for Standard_D2s_v5 (amd64) or Standard_D2ps_v5 (arm64)."
  type        = string
  default     = ""
}

variable "disk_gb" {
  description = "The disk's size, in GiB: the data lives on it."
  type        = number
  default     = 64
}

variable "admin_username" {
  description = "The machine's administrator, signing in by SSH key."
  type        = string
  default     = "openduct-admin"
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

variable "tags" {
  description = "More tags for what is made."
  type        = map(string)
  default     = {}
}
