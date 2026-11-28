# Copyright 2026 Ductile AI, Inc
# SPDX-License-Identifier: Apache-2.0

output "instance_id" {
  description = "The machine."
  value       = aws_instance.openduct.id
}

output "public_ip" {
  description = "Its public address, if its subnet gives one."
  value       = aws_instance.openduct.public_ip
}

output "studio" {
  description = "How to reach the studio."
  value = length(var.studio_cidrs) > 0 ? "http://${aws_instance.openduct.public_ip}:5480" : (
    "ssh -L 5480:127.0.0.1:5480 admin@${aws_instance.openduct.public_ip}, then http://localhost:5480"
  )
}
