# Copyright 2026 Ductile AI, Inc
# SPDX-License-Identifier: Apache-2.0
#
# OpenDuct on one virtual machine on Azure, as OpenTofu sets it up (../README.md): Debian 12, the released program
# as a systemd service, its data on the machine's disk. The studio's port is open only to the addresses given (none
# by default), Kafka's to none; SSH by key only.

terraform {
  required_version = ">= 1.12.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }
}

provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
}

locals {
  arm    = var.arch == "arm64"
  target = local.arm ? "aarch64-unknown-linux-gnu" : "x86_64-unknown-linux-gnu"
  tags   = merge({ "part-of" = "openduct" }, var.tags)
}

resource "azurerm_resource_group" "openduct" {
  name     = "${var.name}-rg"
  location = var.location
  tags     = local.tags
}

resource "azurerm_virtual_network" "openduct" {
  name                = "${var.name}-network"
  address_space       = ["10.42.0.0/16"]
  location            = azurerm_resource_group.openduct.location
  resource_group_name = azurerm_resource_group.openduct.name
  tags                = local.tags
}

resource "azurerm_subnet" "openduct" {
  name                 = "${var.name}-subnet"
  resource_group_name  = azurerm_resource_group.openduct.name
  virtual_network_name = azurerm_virtual_network.openduct.name
  address_prefixes     = ["10.42.1.0/24"]
}

resource "azurerm_public_ip" "openduct" {
  name                = "${var.name}-ip"
  location            = azurerm_resource_group.openduct.location
  resource_group_name = azurerm_resource_group.openduct.name
  allocation_method   = "Static"
  sku                 = "Standard"
  tags                = local.tags
}

resource "azurerm_network_security_group" "openduct" {
  name                = "${var.name}-nsg"
  location            = azurerm_resource_group.openduct.location
  resource_group_name = azurerm_resource_group.openduct.name
  tags                = local.tags

  dynamic "security_rule" {
    for_each = length(var.studio_cidrs) > 0 ? [1] : []
    content {
      name                       = "studio"
      priority                   = 100
      direction                  = "Inbound"
      access                     = "Allow"
      protocol                   = "Tcp"
      source_port_range          = "*"
      destination_port_range     = "5480"
      source_address_prefixes    = var.studio_cidrs
      destination_address_prefix = "*"
    }
  }
  dynamic "security_rule" {
    for_each = length(var.ssh_cidrs) > 0 ? [1] : []
    content {
      name                       = "ssh"
      priority                   = 110
      direction                  = "Inbound"
      access                     = "Allow"
      protocol                   = "Tcp"
      source_port_range          = "*"
      destination_port_range     = "22"
      source_address_prefixes    = var.ssh_cidrs
      destination_address_prefix = "*"
    }
  }
}

resource "azurerm_network_interface" "openduct" {
  name                = "${var.name}-nic"
  location            = azurerm_resource_group.openduct.location
  resource_group_name = azurerm_resource_group.openduct.name
  tags                = local.tags

  ip_configuration {
    name                          = "primary"
    subnet_id                     = azurerm_subnet.openduct.id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.openduct.id
  }
}

resource "azurerm_network_interface_security_group_association" "openduct" {
  network_interface_id      = azurerm_network_interface.openduct.id
  network_security_group_id = azurerm_network_security_group.openduct.id
}

resource "azurerm_linux_virtual_machine" "openduct" {
  name                            = var.name
  location                        = azurerm_resource_group.openduct.location
  resource_group_name             = azurerm_resource_group.openduct.name
  size                            = var.size != "" ? var.size : (local.arm ? "Standard_D2ps_v5" : "Standard_D2s_v5")
  admin_username                  = var.admin_username
  disable_password_authentication = true
  network_interface_ids           = [azurerm_network_interface.openduct.id]
  custom_data = base64encode(templatefile("${path.module}/../vm-install.sh.tftpl", {
    version        = var.openduct_version
    target         = local.target
    studio_hosts   = join(",", var.studio_hosts)
    secure_cookies = var.secure_cookies
  }))
  tags = local.tags

  admin_ssh_key {
    username   = var.admin_username
    public_key = var.ssh_public_key
  }

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "Premium_LRS"
    disk_size_gb         = var.disk_gb
  }

  source_image_reference {
    publisher = "Debian"
    offer     = "debian-12"
    sku       = local.arm ? "12-arm64" : "12-gen2"
    version   = "latest"
  }

  # The start script runs once, at the first boot: a later release is installed with openduct-upgrade on the
  # machine (../README.md), never by making the machine - and its disk - anew.
  lifecycle {
    ignore_changes = [custom_data]
  }
}
