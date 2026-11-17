# Copyright 2026 Ductile AI, Inc
# SPDX-License-Identifier: Apache-2.0
#
# OpenDuct on this machine's Docker, as OpenTofu sets it up (README.md): the same three services as
# deploy/compose - OpenDuct, the S3 store, Postgres with pgvector - with every secret made here, at random.

terraform {
  required_version = ">= 1.12.0"
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "= 4.6.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "= 3.9.1"
    }
  }
}

provider "docker" {}

locals {
  repository = abspath("${path.module}/../../..")
  # The database's first-run script, as deploy/compose has it.
  postgres_init = yamldecode(file("${local.repository}/deploy/compose/compose.yaml")).configs["openduct-postgres"].content
  labels     = { "app.kubernetes.io/part-of" = "openduct" }
}

# Secrets, made once and kept in OpenTofu's state (keep the state private).
resource "random_password" "cluster_secret" {
  length  = 48
  special = false
}
resource "random_password" "s3_secret_key" {
  length  = 40
  special = false
}
resource "random_password" "postgres_password" {
  length  = 32
  special = false
}

resource "docker_network" "openduct" {
  name = "${var.name}-network"
}

resource "docker_volume" "openduct" {
  name = "${var.name}-data"
}
resource "docker_volume" "s3" {
  name = "${var.name}-s3-data"
}
resource "docker_volume" "postgres" {
  name = "${var.name}-postgres-data"
}

resource "docker_image" "openduct" {
  name = var.openduct_image
  dynamic "build" {
    # Built from this repository unless an image is named.
    for_each = var.build_openduct ? [1] : []
    content {
      context    = local.repository
      dockerfile = "Dockerfile"
    }
  }
  keep_locally = true
}
resource "docker_image" "s3" {
  name         = var.s3_image
  keep_locally = true
}
resource "docker_image" "postgres" {
  name         = var.postgres_image
  keep_locally = true
}

resource "docker_container" "s3" {
  name       = "${var.name}-s3"
  image      = docker_image.s3.image_id
  restart    = "unless-stopped"
  entrypoint = ["/bin/sh", "-c"]
  command = [join(" ", [
    "umask 077 &&",
    "printf '{\"identities\":[{\"name\":\"openduct\",\"credentials\":[{\"accessKey\":\"%s\",\"secretKey\":\"%s\"}],\"actions\":[\"Admin\",\"Read\",\"Write\",\"List\",\"Tagging\"]}]}'",
    "\"$(cat /run/secrets/s3_access_key)\" \"$(cat /run/secrets/s3_secret_key)\" > /tmp/s3.json &&",
    "exec weed server -dir=/data -s3 -s3.port=8333 -s3.config=/tmp/s3.json",
  ])]
  # Its keys as files, never in its environment.
  upload {
    file    = "/run/secrets/s3_access_key"
    content = var.s3_access_key
  }
  upload {
    file    = "/run/secrets/s3_secret_key"
    content = random_password.s3_secret_key.result
  }
  networks_advanced {
    name    = docker_network.openduct.name
    aliases = ["s3"]
  }
  volumes {
    volume_name    = docker_volume.s3.name
    container_path = "/data"
  }
  healthcheck {
    test     = ["CMD-SHELL", "nc -z 127.0.0.1 8333"]
    interval = "5s"
    timeout  = "3s"
    retries  = 30
  }
  wait = true
  dynamic "labels" {
    for_each = local.labels
    content {
      label = labels.key
      value = labels.value
    }
  }
}

resource "docker_container" "postgres" {
  name    = "${var.name}-postgres"
  image   = docker_image.postgres.image_id
  restart = "unless-stopped"
  env = [
    "POSTGRES_USER=openduct",
    "POSTGRES_DB=openduct",
    "POSTGRES_PASSWORD_FILE=/run/secrets/postgres_password",
  ]
  upload {
    file    = "/run/secrets/postgres_password"
    content = random_password.postgres_password.result
  }
  networks_advanced {
    name    = docker_network.openduct.name
    aliases = ["postgres"]
  }
  volumes {
    volume_name    = docker_volume.postgres.name
    container_path = "/var/lib/postgresql"
  }
  upload {
    file    = "/docker-entrypoint-initdb.d/openduct.sql"
    content = local.postgres_init
  }
  healthcheck {
    test     = ["CMD-SHELL", "pg_isready -U openduct -d openduct"]
    interval = "5s"
    timeout  = "3s"
    retries  = 30
  }
  wait = true
  dynamic "labels" {
    for_each = local.labels
    content {
      label = labels.key
      value = labels.value
    }
  }
}

resource "docker_container" "openduct" {
  name    = var.name
  image   = docker_image.openduct.image_id
  restart = "unless-stopped"
  env = [
    "OPENDUCT_NODE_ID=1",
    "OPENDUCT_BROKERS=1=${var.name}:9093@localhost:${var.kafka_port}",
    "OPENDUCT_REPLICATION_LISTEN=0.0.0.0:9093",
    "OPENDUCT_REPLICATION_FACTOR=1",
    "OPENDUCT_CLUSTER_SECRET_FILE=/run/secrets/cluster_secret",
    "OPENDUCT_ADVERTISE=localhost:${var.kafka_port}",
    "OPENDUCT_ALLOW_INSECURE=true",
    "OPENDUCT_ALLOW_PRIVATE_NETWORK=true",
    "OPENDUCT_TIER_S3_BUCKET=openduct",
    "OPENDUCT_TIER_S3_ENDPOINT=http://s3:8333",
    "OPENDUCT_TIER_S3_CREATE_BUCKET=true",
    "OPENDUCT_TIER_S3_ACCESS_KEY_FILE=/run/secrets/s3_access_key",
    "OPENDUCT_TIER_S3_SECRET_KEY_FILE=/run/secrets/s3_secret_key",
  ]
  # Its secrets as files, never in its environment.
  upload {
    file    = "/run/secrets/cluster_secret"
    content = random_password.cluster_secret.result
  }
  upload {
    file    = "/run/secrets/s3_access_key"
    content = var.s3_access_key
  }
  upload {
    file    = "/run/secrets/s3_secret_key"
    content = random_password.s3_secret_key.result
  }
  networks_advanced {
    name    = docker_network.openduct.name
    aliases = [var.name]
  }
  ports {
    internal = 5480
    external = var.studio_port
    ip       = "127.0.0.1"
  }
  ports {
    internal = 9092
    external = var.kafka_port
    ip       = "127.0.0.1"
  }
  volumes {
    volume_name    = docker_volume.openduct.name
    container_path = "/data"
  }
  depends_on = [docker_container.s3, docker_container.postgres]
  dynamic "labels" {
    for_each = local.labels
    content {
      label = labels.key
      value = labels.value
    }
  }
}
