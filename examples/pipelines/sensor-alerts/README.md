# Sensor alerts

Temperature and humidity readings from sensors on a factory floor, in Fahrenheit. This pipeline writes each reading
in Celsius, rounded, with an alert level, and keyed by sensor so each sensor's readings stay in order.

| Line | What happens |
|---|---|
| 1 | 72.5 °F is (72.5 − 32) × 5 ÷ 9 = 22.5 °C: `ok`. Humidity 41.6 rounds to 42. |
| 2 | 98.6 °F is 37 °C: `high`. No `enabled` field: a sensor is enabled unless it says otherwise. |
| 3 | 105.8 °F is 41 °C: `critical`. |
| 4 | `enabled: false`: dropped by `where`. |
| 5 | No temperature: `temp_c` is `null` and the alert says `no reading`. Humidity 55.5 rounds half away from zero, to 56. |

```sh
openduct pipeline test pipeline.yaml --input input.jsonl     # prints what expected.jsonl holds
```
