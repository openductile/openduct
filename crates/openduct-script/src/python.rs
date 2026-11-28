// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A Python step: the code runs with `value`, `steps`, `trigger` (and a loop's `item` and `index`) as globals, and
//! its answer is what it sets `result` to; `print` is kept as the step's logs.
//!
//! The interpreter has no host access built in (no `os`, `posix`, `socket`, `select`, `mmap`, `ctypes`, `signal`,
//! file objects or file-system importers), and of the native modules only those that compute are added. The Python
//! half of the standard library is built in too: a module of it that needs what is missing (`os`, `subprocess`,
//! `socket`) fails to import, the rest - json, re, datetime, decimal, statistics, collections, textwrap ... - work.

use rustpython_vm::builtins::PyBaseExceptionRef;
use rustpython_vm::compiler::Mode;
use rustpython_vm::{AsObject, Interpreter, PyObjectRef, Settings, VirtualMachine, py_serde};
use serde_json::Value as Json;

use crate::{Answer, Failure, Script, kept_logs};

/// The native modules a script may import: they compute, and reach nothing outside.
const NATIVE: &[&str] = &[
    "array",
    "binascii",
    "_bisect",
    "_blake2",
    "_bz2",
    "cmath",
    "_contextvars",
    "_csv",
    "_datetime",
    "_hashlib",
    "_heapq",
    "_json",
    "_lzma",
    "math",
    "_md5",
    "_pickle",
    "_random",
    "_sha1",
    "_sha2",
    "_sha256",
    "_sha3",
    "_sha512",
    "_statistics",
    "_struct",
    "unicodedata",
    "zlib",
    "_codecs_cn",
    "_codecs_hk",
    "_codecs_iso2022",
    "_codecs_jp",
    "_codecs_kr",
    "_codecs_tw",
    "_multibytecodec",
    // The opcodes' names and the tokenizer, for dis and tokenize (and so inspect and dataclasses).
    "_opcode",
    "_tokenize",
    // "Did you mean ...?" in an error.
    "_suggestions",
];

/// A stand-in `os`, for the modules that import it (random, statistics, uuid, dataclasses, inspect ...): names and
/// paths only - no files, programs, network or environment - and `urandom` from the system's secure randomness
/// (a seed given in, stretched with SHA-256), so `secrets` and `uuid4` stay sound.
const OS: &str = r#"
def __openduct_install(seed):
    import abc, hashlib, sys, types
    os = types.ModuleType("os", "A script's os: names and paths only. It reaches no files, programs or network.")
    os.name, os.sep, os.altsep, os.curdir, os.pardir, os.extsep = "posix", "/", None, ".", "..", "."
    os.pathsep, os.defpath, os.linesep, os.devnull = ":", "/bin:/usr/bin", "\n", "/dev/null"
    os.environ = {}
    os.error = OSError
    state = [seed, 0]
    def urandom(n):
        out = b""
        while len(out) < n:
            out += hashlib.sha256(state[0] + state[1].to_bytes(8, "big")).digest()
            state[1] += 1
        return out[:n]
    def getenv(key, default=None):
        return default
    def fspath(path):
        if isinstance(path, (str, bytes)):
            return path
        method = getattr(type(path), "__fspath__", None)
        if method is None:
            raise TypeError("expected str, bytes or os.PathLike object, not " + type(path).__name__)
        return method(path)
    def fsencode(name):
        name = fspath(name)
        return name if isinstance(name, bytes) else name.encode("utf-8", "surrogateescape")
    def fsdecode(name):
        name = fspath(name)
        return name if isinstance(name, str) else name.decode("utf-8", "surrogateescape")
    def cpu_count():
        return 1
    def getpid():
        return 1
    def getcwd():
        return "/"
    def missing(name):
        raise AttributeError("a script has no os." + name + ": it reaches no files, programs or network")
    class PathLike(abc.ABC):
        @classmethod
        def __subclasshook__(cls, subclass):
            return hasattr(subclass, "__fspath__") or NotImplemented
    for f in (urandom, getenv, fspath, fsencode, fsdecode, cpu_count, getpid, getcwd):
        setattr(os, f.__name__, f)
    os.PathLike = PathLike
    os.__getattr__ = missing
    sys.modules["os"] = os
    # importlib's own machinery takes its os by this name.
    sys.modules["posix"] = os
    import posixpath
    os.path = posixpath
    sys.modules["os.path"] = posixpath
__openduct_install(__openduct_seed)
del __openduct_install, __openduct_seed
"#;

/// What `print` writes, kept.
const PRELUDE: &str = r#"
import sys as __openduct_sys
class __OpenductOut:
    def __init__(self):
        self.parts = []
    def write(self, text):
        self.parts.append(str(text))
        return len(text)
    def flush(self):
        pass
    def text(self):
        return "".join(self.parts)
__openduct_out = __OpenductOut()
__openduct_sys.stdout = __openduct_sys.stderr = __openduct_out
del __openduct_sys
"#;

fn interpreter() -> Interpreter {
    let mut settings = Settings::default();
    settings.import_site = false;
    settings.isolated = true;
    settings.safe_path = true;
    settings.user_site_directory = false;
    settings.install_signal_handlers = false;
    settings.write_bytecode = false;
    settings.faulthandler = false;
    let builder = Interpreter::builder(settings);
    let native: Vec<_> = rustpython_stdlib::stdlib_module_defs(&builder.ctx)
        .into_iter()
        .filter(|def| NATIVE.contains(&def.name.as_str()))
        .collect();
    builder
        .add_native_modules(&native)
        .add_frozen_modules(rustpython_pylib::FROZEN_STDLIB.decode())
        .build()
}

/// A script that asked for more memory than its process may have (its limit is set in [`crate::Isolated`]).
const OUT_OF_MEMORY: &str = "the script used more memory than a script may: it was stopped";

/// What the person sees for a script's error: Python's own text, except running out of memory, which is said plainly.
fn shown(vm: &VirtualMachine, error: &PyBaseExceptionRef) -> String {
    if error.fast_isinstance(vm.ctx.exceptions.memory_error) {
        return OUT_OF_MEMORY.to_owned();
    }
    let mut text = String::new();
    if vm.write_exception(&mut text, error).is_err() || text.trim().is_empty() {
        return "the script failed".to_owned();
    }
    text.trim_end().to_owned()
}

fn logs(vm: &VirtualMachine, out: Option<PyObjectRef>) -> Vec<String> {
    let Some(out) = out else { return Vec::new() };
    let joined: String = vm
        .call_method(&out, "text", ())
        .and_then(|text| text.try_into_value(vm))
        .unwrap_or_default();
    kept_logs(joined.lines().map(str::to_owned).collect())
}

pub(crate) fn run(script: &Script) -> Result<Answer, Failure> {
    interpreter().enter(|vm| {
        let scope = vm.new_scope_with_builtins();
        let fail = |vm: &VirtualMachine, e: &PyBaseExceptionRef| Failure::new(shown(vm, e));
        for (name, value) in &script.names {
            let object = py_serde::deserialize(vm, value.clone())
                .map_err(|e| Failure::new(format!("{name} cannot be given to Python: {e}")))?;
            scope
                .globals
                .set_item(name.as_str(), object, vm)
                .map_err(|e| fail(vm, &e))?;
        }
        let mut seed = [0u8; 32];
        ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut seed)
            .map_err(|_| Failure::new("cannot read the system's randomness"))?;
        scope
            .globals
            .set_item("__openduct_seed", vm.ctx.new_bytes(seed.to_vec()).into(), vm)
            .map_err(|e| fail(vm, &e))?;
        for (part, source) in [("<os>", OS), ("<prelude>", PRELUDE)] {
            let code = vm
                .compile(source, Mode::Exec, part)
                .map_err(|e| Failure::new(e.to_string()))?;
            vm.run_code_obj(code, scope.clone()).map_err(|e| fail(vm, &e))?;
        }
        let out = scope.globals.get_item_opt("__openduct_out", vm).ok().flatten();
        let code = match vm.compile(&script.code, Mode::Exec, "<step>") {
            Ok(code) => code,
            Err(e) => return Err(Failure::new(format!("SyntaxError: {e}"))),
        };
        if let Err(e) = vm.run_code_obj(code, scope.clone()) {
            return Err(Failure {
                error: shown(vm, &e),
                logs: logs(vm, out),
            });
        }
        let result = scope.globals.get_item_opt("result", vm).ok().flatten();
        let json = match result {
            None => Json::Null,
            Some(object) => match serde_json::to_value(py_serde::PyObjectSerializer::new(vm, &object)) {
                Ok(json) => json,
                Err(e) => {
                    return Err(Failure {
                        error: format!("result cannot be JSON: {e}"),
                        logs: logs(vm, out),
                    });
                }
            },
        };
        // data tables: what the script left in `state`, when it was given one.
        let state = if script.names.contains_key(crate::STATE) {
            match scope.globals.get_item_opt(crate::STATE, vm).ok().flatten() {
                None => Some(Json::Null),
                Some(object) => match serde_json::to_value(py_serde::PyObjectSerializer::new(vm, &object)) {
                    Ok(json) => Some(json),
                    Err(e) => {
                        return Err(Failure {
                            error: format!("state cannot be JSON: {e}"),
                            logs: logs(vm, out),
                        });
                    }
                },
            }
        } else {
            None
        };
        Ok(Answer {
            result: json,
            logs: logs(vm, out),
            state,
        })
    })
}
