// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Python steps' program: `openduct` starts it for each Python script, as it starts itself for each JavaScript one.
//! Installed beside `openduct` (or named by OPENDUCT_PYTHON_PROGRAM). It links RustPython's integers (malachite,
//! LGPL-3.0-only); `openduct` itself does not.

fn main() -> std::process::ExitCode {
    openduct_script::child::main()
}
