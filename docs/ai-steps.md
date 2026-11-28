# AI steps

Every AI step takes an `ai` credential - a model's address, its key and its API's shape - and records the same
things in a run: the model's answer, `model`, `usage` (tokens in and out), the `transcript` (every message sent, then
the answer) and `metrics` (latency, tokens a second). A **test run** asks no model: each step shows the request it
would send, with the key as `•••`, and a stand-in for the answer. A **preview** asks the model for real.

| Step | What it gives |
|---|---|
| **Ask AI** (`ai`) | The model's answer, in one form whatever the model; with a `schema`, JSON held to it |
| **Classify text** (`classify`) | The `category` it chose (or `categories`), and that category's steps run |
| **Sentiment** (`sentiment`) | A `label` (positive, neutral, negative, or your own) and a `score`, and the label's steps run |
| **Extract fields** (`extract`) | `json`: named fields with types pulled out of a text |
| **Summarise** (`summarise`) | A `summary` of a text, or of a list of texts, however long |
| **Embed text** (`embed`) | Vectors, for search |

Beside their own settings, classify, sentiment, extract and summarise take the model's: `credential`, `model` (the
credential's when left out), `temperature`, `top_p`, `max_tokens`, and `instructions` - more for the model to go by,
beside the step's own. A setting the model refuses (`temperature`, say) is left out and the model asked again.

## Answers held to a schema

Classify, sentiment and extract - and Ask AI with a `schema` - ask for JSON and check it:

1. The answer is read leniently: JSON alone, in a fenced block, or with words around it.
2. It is checked against its JSON schema.
3. On a mismatch the model is asked **once more**, told what is wrong (`$.amount: number or null, not a text`).
4. A second miss fails the step, as a model that does not answer does: its retries and `on_error` apply.

Both calls are in the step's `transcript`, their tokens in its `usage`, and `metrics.repairs` is 1.

The schema keywords checked: `type`, `enum`, `const`, `properties`, `required`, `additionalProperties`, `items`,
`minItems`, `maxItems`, `uniqueItems`, `minLength`, `maxLength`, `pattern`, `minimum`, `maximum`, `exclusiveMinimum`,
`exclusiveMaximum`, `multipleOf`, `anyOf`, `oneOf`, `allOf`, `nullable`, `format` (`date`, `date-time` and `email` are
checked), and `$ref` to the schema's own `$defs`. `description`, `title`, `examples` and `default` are notes for the
model. Any other keyword is refused when the workflow is checked, rather than passed over. A schema is at most 64 KiB.

```yaml
- name: plan
  ai:
    credential: model
    prompt: "Plan {{ value.days }} days in {{ value.city }}"
    schema:
      type: object
      properties:
        days: { type: array, items: { type: string }, minItems: 1 }
      required: [days]
# steps.plan.json.days
```

## Classify text

```yaml
- name: sort
  classify:
    credential: model
    text: "{{ value.subject }}. {{ value.text }}"
    categories:
      - name: refund
        description: Asks for money back
        steps:
          - topic: { name: refunds, value: "{{ value }}" }
      - name: bug
        description: Something does not work
        steps:
          - slack: { credential: team, channel: "#bugs", text: "{{ value.text }}" }
      - question            # a name alone: no description, no steps
    default:                # when none fits: the category is other
      - topic: { name: support.other, value: "{{ value }}" }
```

- `categories`: 1 to 50, each a name or `{name, description, steps}`; names are unique (whatever their case).
- `other` (true unless set to false): the model may answer `other` when none fits, and `default`'s steps run. With
  `other: false` it must choose one of the categories.
- `several: true`: every category that fits, in `categories`; each one's steps run, in the order they are listed.
- The result: `category` (the first chosen), `categories` (all of them). A name the model gives in another case is the
  category's own; anything else is asked again.
- On the canvas, each category is a branch with its own steps, and other is the last one.

## Sentiment

```yaml
- name: mood
  sentiment:
    credential: model
    text: "{{ value.review }}"
    labels:                 # optional: positive, neutral, negative when left out
      - positive
      - neutral
      - name: negative
        description: Unhappy, angry or disappointed
        steps:
          - slack: { credential: team, channel: "#care", text: "Unhappy: {{ value.review }}" }
```

The result: `label`, and `score` - how strongly the text carries it, from 0 to 1. Labels are 2 to 50, as categories
are; each label's steps run when it is the one.

## Extract fields

```yaml
- name: lead
  extract:
    credential: model
    text: "{{ value.text }}"
    fields:
      - { name: name, description: "the person's full name" }
      - { name: email, type: string, required: true }
      - { name: seats, type: integer, description: how many they ask for }
      - { name: start, type: date }
      - { name: wants, type: list }
# steps.lead.json.email
```

- `fields`: 1 to 100, each a name or `{name, type, description, required}`. Types: `string` (the default), `number`,
  `integer`, `boolean`, `date` (as 2026-11-27) and `list` (of texts).
- A field the text does not give is `null`, unless it is `required`, when the model is asked again and the step fails
  if it still is not there. A field the model adds that the step does not name is dropped.
- Or `schema`: a JSON schema of your own in place of `fields` (lists of records, nested ones), checked as it is
  written.

## Summarise

```yaml
- name: digest
  summarise:
    credential: model
    text: "{{ value.messages }}"   # a text, or a list of texts
    length: short                  # short, medium, long - or your own words: 3 sentences, under 50 words
    style: bullets                 # bullets, prose - or your own words: for a customer, warmly
```

What fits in one piece (`piece_size`: 12,000 characters unless set, 1,000 to 100,000) is summarised in one call. A
longer input is cut into pieces at paragraph or sentence ends, each piece noted (four at a time), the notes combined
again while they are too long for one piece, and the summary written from them. The result: `summary`, `pieces`, and
`metrics.calls`; `usage` counts every call, `transcript` is the summary's own. An input of more than 40 pieces is
refused, with what to change.

## Limits

- One model call is given at most 200,000 characters of text.
- Categories and labels: at most 50; extract's fields: at most 100.
- One repair turn per call; summarise makes at most 40 piece calls, and a few more to combine their notes.
- A step's result, as any step's, is at most 4 MiB.

## Services

An AI credential starts from one of these, and any service that speaks the chat completions API (or the messages
API) works too, with its address:

OpenAI, Anthropic, Google Gemini, Mistral AI, Groq, OpenRouter, Together AI, Azure OpenAI, Amazon Bedrock, DeepSeek,
xAI, and on this machine Ollama and LM Studio.

- **Azure OpenAI**: the address is your resource's (`https://<resource>.openai.azure.com/openai/v1`), the key goes in
  the `api-key` header, and a step's model is your deployment's name.
- **Amazon Bedrock**: its OpenAI-compatible endpoint in your region
  (`https://bedrock-runtime.<region>.amazonaws.com/openai/v1`) with a Bedrock API key. It lists no models, so a step
  names its model (as `openai.gpt-oss-120b-1:0`).
- **Google Vertex AI** is not one of them: its compatible endpoint signs in with a token that lasts an hour, not a key
  a credential keeps. Gemini models are reached with a Gemini API key instead.
