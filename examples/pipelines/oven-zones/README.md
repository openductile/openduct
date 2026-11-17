# Oven zones

A curing oven's PLC on the factory network, read over OPC UA, signed and encrypted. The pipeline watches the
temperature of the oven's three zones and whether its door is open, and writes each change as it happens, keyed
by the `device` name: a zone's reading with whether it is within the recipe's band (180 °C, give or take 5), the
door as it is. A reading the PLC marks as not good (a failed sensor, say) is left out. A zone that moves by less
than half a degree is not reported (`deadband`).

The sample input is what the connector writes, one change per line.

| Line | What happens |
|---|---|
| 1 | Zone 1 at 179.62 °C: 179.6, in the band. |
| 2 | Zone 2 at 186.04 °C: 186, out of the band. |
| 3 | Zone 3's sensor has failed (`BadSensorFailure`): left out by `where`. |
| 4 | The door opens: `true`. |

```sh
openduct pipeline test pipeline.yaml --input input.jsonl     # prints what expected.jsonl holds
```

To run it, ask the PLC's integrator for the endpoint address, a user for OpenDuct, and the node ids of the values
(an OPC UA browser shows them). Set `OVEN_USER` and `OVEN_PASSWORD`, create the topic, and run the pipeline once:

```sh
openduct topic create oven-2
openduct pipeline run pipeline.yaml
```

The first run makes OpenDuct's own certificate (in `opcua-pki/own`) and stops: the oven's certificate is not
trusted yet. Check it with the integrator and move it from `opcua-pki/rejected` to `opcua-pki/trusted`; the PLC
must trust OpenDuct's certificate the same way. Then run the pipeline again.
