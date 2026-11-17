# Connecting forms

A form reaches a workflow through its **webhook**: the form tool calls the workflow's address each time someone
answers. Any form tool that can call a webhook works; this page shows the common ones, and a form on your own site.

## What every form needs

1. A workflow whose trigger is a **Webhook**, with a token ([triggers](triggers.md#webhook-another-system-calls-its-address)).
2. The studio reachable from the internet - behind HTTPS, named with `--studio-host` - so the form tool can call it
   ([deployment](../deployment.md#the-studio-and-workflows-in-a-cluster)).
3. The address to give the form tool: the workflow's address with the token added, as
   `https://flows.example.com/hooks/01a1e8a94aba7177bdd8d2734a236a7e?token=your-own-token`.
4. A sample input shaped like what the form sends. The surest way: answer the form once with the workflow on, and
   copy the event from **Runs** into **Sample input**.

OpenDuct reads what the form sends whichever way it comes: JSON as it is, and a form's fields (`urlencoded` or
`multipart`) one by one, a file by its name. Each field is then `value.<name>` in the steps.

## Typeform

**Connect → Webhooks → Add a webhook**, paste the address, and turn it on. **Send test request** sends a sample.

Typeform sends JSON: the answers are a list, each with the `ref` of its question (set it under the question's
settings). Pick them out with a **Set fields** step:

```text
email:   value.form_response.answers.filter(a, a.field.ref == 'email')[0].email
name:    value.form_response.answers.filter(a, a.field.ref == 'name')[0].text
choice:  value.form_response.answers.filter(a, a.field.ref == 'product')[0].choice.label
```

The value is under the answer's type: `text`, `email`, `number`, `boolean`, `date`, `choice.label`, `choices.labels`.
[Your first workflow](first-workflow.md) builds a whole one from a Typeform answer.

## Tally

**Integrations → Webhooks → Connect**, and paste the address. Tally sends JSON with the answers in `data.fields`, each
with its question's label:

```text
email:   value.data.fields.filter(f, f.label == 'Email')[0].value
name:    value.data.fields.filter(f, f.label == 'Name')[0].value
```

## Jotform

**Settings → Integrations → WebHooks**, and paste the address. Jotform sends a form whose `rawRequest` field holds
the answers as JSON, each named after its question - such as `q3_email`, or `q4_name` with its `first` and `last`.
Read them with `json.decode`:

```text
email:   json.decode(value.rawRequest).q3_email
first:   json.decode(value.rawRequest).q4_name.first
```

Answer the form once and open the run: the event shows each name Jotform used.

## Google Forms

Google Forms calls no webhook itself; a few lines of Apps Script, run on each answer, do. In the form, **⋮ → Script
editor**, paste this, and set the address:

```js
const ADDRESS = "https://flows.example.com/hooks/01a1e8a94aba7177bdd8d2734a236a7e?token=your-own-token";

function onAnswer(e) {
  const answers = {};
  for (const item of e.response.getItemResponses()) {
    answers[item.getItem().getTitle()] = item.getResponse();
  }
  UrlFetchApp.fetch(ADDRESS, {
    method: "post",
    contentType: "application/json",
    payload: JSON.stringify({ email: e.response.getRespondentEmail(), answers: answers }),
  });
}
```

Then **Triggers → Add trigger**: function `onAnswer`, event source *From form*, event type *On form submit*. Each
answer is then `value.answers['Your question']`.

## Microsoft Forms

Microsoft Forms calls no webhook itself either. A Power Automate flow - *When a new response is submitted*, then *Get
response details*, then an *HTTP* action posting the details as JSON to the address - does it.

## A form on your own site

A plain HTML form can post straight to the workflow:

```html
<form method="post" action="https://flows.example.com/hooks/01a1e8a94aba7177bdd8d2734a236a7e">
  <input name="name" required>
  <input name="email" type="email" required>
  <textarea name="message"></textarea>
  <button>Send</button>
</form>
```

Its fields arrive as `value.name`, `value.email` and `value.message`. Two things differ from a form tool:

- **A token in a public page protects nothing** - anyone can read it in the page. Leave the webhook without one, and
  check what arrives instead: a **Filter** step (`value.email != null && value.email.contains('@')`) stops the rest.
- **The person sees the answer.** End with a **Respond to webhook** step that sends them back to your site: status
  `303`, header `Location: https://shop.example.com/thanks`.

## Other form tools

WPForms, Gravity Forms, Webflow, Wix, Formspree, Fillout and others can call a webhook on each answer (some on paid
plans). Give them the address with `?token=`, answer once, and copy the event into the sample: whatever shape they
send, the steps read it as `value`.
