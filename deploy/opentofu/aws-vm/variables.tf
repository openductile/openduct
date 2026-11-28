# Copyright 2026 Ductile AI, Inc
# SPDX-License-Identifier: Apache-2.0

variable "name" {
  description = "The name of the machine and of what is made with it."
  type        = string
  default     = "openduct"
}

variable "region" {
  description = "The AWS region."
  type        = string
  default     = "eu-central-1"
}

variable "openduct_version" {
  description = "The OpenDuct release to run, as v0.1.2."
  type        = string
  default     = "v0.1.2"
}

variable "arch" {
  description = "amd64 or arm64 (Graviton)."
  type        = string
  default     = "arm64"
  validation {
    condition     = contains(["amd64", "arm64"], var.arch)
    error_message = "arch: amd64 or arm64."
  }
}

variable "instance_type" {
  description = "The instance type; empty for t4g.medium (arm64) or t3.medium (amd64)."
  type        = string
  default     = ""
}

variable "disk_gb" {
  description = "The disk's size, in GiB: the data lives on it."
  type        = number
  default     = 40
}

variable "subnet_id" {
  description = "The subnet to start the machine in; empty for one of the default VPC's."
  type        = string
  default     = ""
}

variable "key_name" {
  description = "An EC2 key pair for SSH; empty for none."
  type        = string
  default     = ""
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
