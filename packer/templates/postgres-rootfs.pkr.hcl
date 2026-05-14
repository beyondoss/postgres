packer {
  required_plugins {
    docker = {
      version = ">= 1.0.0"
      source  = "github.com/hashicorp/docker"
    }
  }
}

variable "ubuntu_version" {
  type        = string
  default     = "noble"
  description = "Ubuntu codename (noble)"

  validation {
    condition     = contains(["noble"], var.ubuntu_version)
    error_message = "The ubuntu_version must be noble (24.04)."
  }
}

variable "postgres_version" {
  type        = string
  default     = "18"
  description = "Postgres major version"
}

variable "output_dir" {
  type        = string
  default     = "/beyond/images/postgres"
  description = "Directory for output images"
}

variable "image_version" {
  type        = string
  default     = ""
  description = "Image version tag (defaults to git short SHA)"
}

variable "target_arch" {
  type        = string
  default     = ""
  description = "Target architecture (arm64, amd64). Auto-detected if empty."
}

variable "build_tiers" {
  type        = string
  default     = "16g"
  description = "Space-separated list of tiers to build. Defaults to 16g (read-only rootfs fits easily)."
}

# Beyond sibling extension git sources — passed from mise task via extensions.toml.
variable "auth_ext_git"   { type = string; default = "" }
variable "auth_ext_tag"   { type = string; default = "" }
variable "queue_ext_git"  { type = string; default = "" }
variable "queue_ext_tag"  { type = string; default = "" }
variable "pgvector_version"       { type = string; default = "" }
variable "pgvectorscale_version"  { type = string; default = "" }
variable "postgis_version"        { type = string; default = "" }
variable "pg_cron_version"        { type = string; default = "" }
variable "pg_partman_version"     { type = string; default = "" }
variable "pg_jsonschema_version"  { type = string; default = "" }
variable "hypopg_version"         { type = string; default = "" }
variable "pg_repack_version"      { type = string; default = "" }
variable "pg_search_version"      { type = string; default = "" }

locals {
  image_name    = "postgres-${var.ubuntu_version}"
  image_version = var.image_version != "" ? var.image_version : "latest"
  target_arch   = var.target_arch != "" ? var.target_arch : "amd64"
}

source "docker" "ubuntu" {
  image    = "ubuntu:${var.ubuntu_version}"
  platform = "linux/${local.target_arch}"
  commit   = true

  run_command = [
    "-d",
    "-i",
    "-t",
    "--privileged",
    "--network=host",
    "--cap-add=SYS_ADMIN",
    "{{.Image}}",
    "/bin/bash"
  ]
}

build {
  name = "postgres-rootfs"

  sources = ["source.docker.ubuntu"]

  # Stage pre-built musl static binaries. Built by `mise run build:image`
  # via cross-compilation before Packer runs — no Rust toolchain needed in the image.
  provisioner "file" {
    source      = "${path.root}/../staging/beyond-pg-bin/"
    destination = "/tmp/beyond-pg-bin"
  }

  # Config file templates — rootfs copies for operator inspection.
  # beyond-pg boot writes the embedded copies to PGDATA at every boot;
  # Postgres never reads these /etc paths directly.
  provisioner "file" {
    source      = "${path.root}/../files/postgresql/00-beyond.conf"
    destination = "/tmp/00-beyond.conf"
  }
  provisioner "file" {
    source      = "${path.root}/../files/postgresql/pg_hba.conf"
    destination = "/tmp/pg_hba.conf"
  }
  provisioner "file" {
    source      = "${path.root}/../files/pgbouncer/pgbouncer.ini"
    destination = "/tmp/pgbouncer.ini"
  }

  provisioner "shell" {
    environment_vars = [
      "DEBIAN_FRONTEND=noninteractive",
      "UBUNTU_VERSION=${var.ubuntu_version}",
      "POSTGRES_VERSION=${var.postgres_version}",
      "TARGET_ARCH=${local.target_arch}",
      "AUTH_EXT_GIT=${var.auth_ext_git}",
      "AUTH_EXT_TAG=${var.auth_ext_tag}",
      "QUEUE_EXT_GIT=${var.queue_ext_git}",
      "QUEUE_EXT_TAG=${var.queue_ext_tag}",
      "PGVECTOR_VERSION=${var.pgvector_version}",
      "PGVECTORSCALE_VERSION=${var.pgvectorscale_version}",
      "POSTGIS_VERSION=${var.postgis_version}",
      "PG_CRON_VERSION=${var.pg_cron_version}",
      "PG_PARTMAN_VERSION=${var.pg_partman_version}",
      "PG_JSONSCHEMA_VERSION=${var.pg_jsonschema_version}",
      "HYPOPG_VERSION=${var.hypopg_version}",
      "PG_REPACK_VERSION=${var.pg_repack_version}",
      "PG_SEARCH_VERSION=${var.pg_search_version}",
    ]
    scripts = [
      "${path.root}/../scripts/01-base-packages.sh",
      "${path.root}/../scripts/02-postgres-install.sh",
      "${path.root}/../scripts/03-pgdg-extensions.sh",
      "${path.root}/../scripts/04-beyond-extensions.sh",
      "${path.root}/../scripts/05-pgbouncer-install.sh",
      "${path.root}/../scripts/06-beyond-pg-install.sh",
      "${path.root}/../scripts/07-config.sh",
      "${path.root}/../scripts/08-mmds.sh",
      "${path.root}/../scripts/09-cleanup.sh",
    ]
  }

  post-processors {
    post-processor "docker-tag" {
      repository = "beyond-postgres-rootfs"
      tags       = ["${local.image_name}-${local.image_version}"]
    }

    post-processor "shell-local" {
      environment_vars = [
        "IMAGE_NAME=${local.image_name}",
        "IMAGE_VERSION=${local.image_version}",
        "OUTPUT_DIR=${var.output_dir}",
        "DOCKER_TAG=beyond-postgres-rootfs:${local.image_name}-${local.image_version}",
        "BUILD_TIERS=${var.build_tiers}",
      ]
      script = "${path.root}/../scripts/post-process.sh"
    }
  }
}
