# Machine states

Presses on a factory floor publish their state to an MQTT broker, one topic per machine
(`factory/<site>/press/<machine>/state`). This pipeline reads every press with a QoS 1 subscription and writes
each state with a plain word for what the machine is doing, keyed by machine so one machine's states stay in
order. A state is acknowledged to the MQTT broker only once it is in the `machine-states` topic.

| Line | What happens |
|---|---|
| 1 | Turning, no fault: `running`. 1180.4 rpm rounds to 1180. |
| 2 | Not turning, no fault: `idle`. |
| 3 | Fault code 214: `fault`, whatever the speed, and the code is kept. |
| 4 | Another line's press, running. Its line is written in capitals, as the others are. |

Try the expressions on the sample, with no MQTT broker needed:

```sh
openduct pipeline test pipeline.yaml --input input.jsonl     # prints what expected.jsonl holds
```

To run it, set `MQTT_USER` and `MQTT_PASSWORD`, point `url` at your MQTT broker, and create the topic:

```sh
openduct topic create machine-states
openduct pipeline run pipeline.yaml
```
