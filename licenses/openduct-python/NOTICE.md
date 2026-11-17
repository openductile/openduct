# openduct-python: what it is made of

`openduct-python` is the program that runs OpenDuct's Python steps. OpenDuct starts it for each Python script, and
it is installed beside `openduct`. Its own code is OpenDuct's, under the Apache License 2.0. It is built with these
parts, under their own licences:

| Part | What it does | Licence |
| --- | --- | --- |
| RustPython (`rustpython-vm`, `rustpython-stdlib`, `rustpython-compiler` and the crates they build on) | the Python interpreter | MIT |
| The Python standard library's Python modules, as `rustpython-pylib` carries them | json, re, datetime and the rest | Python Software Foundation License 2.0 (`PSF-LICENSE.txt`) |
| malachite (`malachite-base`, `malachite-nz`, `malachite-q`, `malachite-bigint`) | Python's integers of any size | GNU Lesser General Public License 3.0 only (`LGPL-3.0.txt`, which builds on `GPL-3.0.txt`) |

## malachite, under the LGPL

malachite is linked into `openduct-python` unchanged. You may replace it with a version of your own, as the LGPL
allows:

1. Take malachite's source from crates.io (<https://crates.io/crates/malachite>) and change it as you like.
2. Point this repository's build at your copy, with a `[patch.crates-io]` entry in `Cargo.toml`.
3. Build the program again from this repository's source, which is all public:

   ```sh
   cargo build --release -p openduct-script --bin openduct-python
   ```

`openduct` itself links no part of malachite or RustPython. It only starts `openduct-python` as a separate program.
