# Fault tickets

The maintenance team works in ServiceNow. This pipeline reads the presses' states from the `machine-states` topic
(which the [machine-states example](../machine-states) writes) and opens an incident when a press is stopped by a
fault. A fault the press reports again that day changes the same incident instead of opening another: the
incident is found by its `correlation_id`, made of the machine, the fault code and the day.

| Line | What happens |
|---|---|
| 1 | Press 7 running: left out by `where`. |
| 2 | Press 7 stopped by fault 214: an incident is opened. |
| 3 | Press 11 idle: left out. |
| 4 | Press 7 reports fault 214 again: the same incident is changed, with the new time. |
| 5 | Press 11 stopped by fault 301: a second incident. |

Try the expressions on the sample, with no ServiceNow instance needed:

```sh
openduct pipeline test pipeline.yaml --input input.jsonl     # prints what expected.jsonl holds
```

To run it, give an integration user who may create and change incidents (the `itil` role, or one made for this).
Set `SN_USER` and `SN_PASSWORD`, put the instance's address in `instance`, and create the topic for incidents
ServiceNow refuses:

```sh
openduct topic create fault-tickets-refused
openduct pipeline run pipeline.yaml
```
