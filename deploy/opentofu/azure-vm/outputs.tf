# Copyright 2026 Ductile AI, Inc
# SPDX-License-Identifier: Apache-2.0

output "vm_id" {
  description = "The machine."
  value       = azurerm_linux_virtual_machine.openduct.id
}

output "public_ip" {
  description = "Its public address."
  value       = azurerm_public_ip.openduct.ip_address
}

output "studio" {
  description = "How to reach the studio."
  value = length(var.studio_cidrs) > 0 ? "http://${azurerm_public_ip.openduct.ip_address}:5480" : (
    "ssh -L 5480:127.0.0.1:5480 ${var.admin_username}@${azurerm_public_ip.openduct.ip_address}, then http://localhost:5480"
  )
}
