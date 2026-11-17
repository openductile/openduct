# Energy meter

A panel's energy meter on the factory network, read over Modbus TCP every 10 seconds. Voltage, current and power
are 32-bit floats in input registers; the energy count is a 32-bit whole number in hundredths of a kWh; a discrete
input says whether the main breaker is closed. The pipeline writes power in kW with a load level, keyed by the
`device` name, and writes a reading only when something changed (`changes_only`).

The sample input is what the connector reads from the meter, one reading per line.

| Line | What happens |
|---|---|
| 1 | 12.61 A: `normal`. 2915.6 W is 2.92 kW, rounded. |
| 2 | 27.04 A, at or over 25: `high`. |
| 3 | 33.5 A, at or over 32: `over`. |
| 4 | The breaker is open: `off`, whatever the readings. |

```sh
openduct pipeline test pipeline.yaml --input input.jsonl     # prints what expected.jsonl holds
```

Check your meter's register map for the addresses: they are the addresses in the request, from 0. A register
documented as `30001` is usually input register `0`, and `40101` is holding register `100`.
