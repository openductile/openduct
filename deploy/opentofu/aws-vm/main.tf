# Copyright 2026 Ductile AI, Inc
# SPDX-License-Identifier: Apache-2.0
#
# OpenDuct on one virtual machine on AWS (EC2), as OpenTofu sets it up (../README.md): Debian 12, the released
# program as a systemd service, its data on the machine's encrypted disk. The studio's port is open only to the
# addresses given (none by default: reach it through an SSH tunnel, or put a TLS proxy in front), Kafka's to none.

terraform {
  required_version = ">= 1.12.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = var.region
}

locals {
  arm    = var.arch == "arm64"
  target = local.arm ? "aarch64-unknown-linux-gnu" : "x86_64-unknown-linux-gnu"
  tags   = merge({ "app.kubernetes.io/part-of" = "openduct", Name = var.name }, var.tags)
}

# Debian's own images, published by the Debian cloud team's account.
data "aws_ami" "debian" {
  most_recent = true
  owners      = ["136693071363"]
  filter {
    name   = "name"
    values = [local.arm ? "debian-12-arm64-*" : "debian-12-amd64-*"]
  }
  filter {
    name   = "architecture"
    values = [local.arm ? "arm64" : "x86_64"]
  }
}

data "aws_vpc" "default" {
  count   = var.subnet_id == "" ? 1 : 0
  default = true
}

data "aws_subnets" "default" {
  count = var.subnet_id == "" ? 1 : 0
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default[0].id]
  }
}

data "aws_subnet" "chosen" {
  id = var.subnet_id != "" ? var.subnet_id : sort(data.aws_subnets.default[0].ids)[0]
}

resource "aws_security_group" "openduct" {
  name_prefix = "${var.name}-"
  description = "OpenDuct: the studio's port to the addresses given, SSH to those given"
  vpc_id      = data.aws_subnet.chosen.vpc_id
  tags        = local.tags

  dynamic "ingress" {
    for_each = length(var.studio_cidrs) > 0 ? [1] : []
    content {
      description = "The studio and webhooks"
      from_port   = 5480
      to_port     = 5480
      protocol    = "tcp"
      cidr_blocks = var.studio_cidrs
    }
  }
  dynamic "ingress" {
    for_each = length(var.ssh_cidrs) > 0 ? [1] : []
    content {
      description = "SSH"
      from_port   = 22
      to_port     = 22
      protocol    = "tcp"
      cidr_blocks = var.ssh_cidrs
    }
  }
  # Out: the release's download, and whatever the workflows call.
  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }
}

resource "aws_instance" "openduct" {
  ami                    = data.aws_ami.debian.id
  instance_type          = var.instance_type != "" ? var.instance_type : (local.arm ? "t4g.medium" : "t3.medium")
  subnet_id              = data.aws_subnet.chosen.id
  vpc_security_group_ids = [aws_security_group.openduct.id]
  key_name               = var.key_name != "" ? var.key_name : null
  user_data = templatefile("${path.module}/../vm-install.sh.tftpl", {
    version        = var.openduct_version
    target         = local.target
    studio_hosts   = join(",", var.studio_hosts)
    secure_cookies = var.secure_cookies
  })
  user_data_replace_on_change = false

  root_block_device {
    volume_size = var.disk_gb
    volume_type = "gp3"
    encrypted   = true
  }
  # Instance metadata only with a session token (IMDSv2), from the machine itself.
  metadata_options {
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }
  tags = local.tags

  # The start script runs once, at the first boot: a later release is installed with openduct-upgrade on the
  # machine (../README.md), never by making the machine - and its disk - anew.
  lifecycle {
    ignore_changes = [ami, user_data]
  }
}
