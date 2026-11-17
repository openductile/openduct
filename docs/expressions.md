# Expressions

An expression is a short piece of text that works on a message: it picks out fields, cleans them up, calculates
and decides. A [pipeline](pipelines.md) uses expressions to change messages as they flow from one topic to
another, and some [connectors](connectors.md) use them to find records in what they read.

```yaml
where: status != 'test'
map:
  order_id: id
  customer: customer.name.trim()
  total:    round(lines.map(l, l.qty * l.price).sum(), 2)
  placed:   timestamp(created_at, '%Y-%m-%d %H:%M:%S')
  status:   "paid ? 'Paid' : 'Open'"
```

Expressions are written in [CEL](https://cel.dev), the Common Expression Language: an open standard, so what
you learn here holds wherever CEL is used, and guides to CEL apply here too. An expression never loops forever,
never touches files or the network, and never changes the message it reads. It gives an answer, or a clear reason
why it cannot.

## Writing them in a pipeline file

Text inside an expression goes in single quotes: `status != 'test'`.

A pipeline file is YAML, and YAML reads some characters itself. Put the whole expression in double quotes when it
has a colon followed by a space (as `a ? b : c` does) or ` #`, or when it starts with a quote, `!`, `[`, `{`, `*`,
`&`, `?`, `|`, `>`, `%` or `@`:

```yaml
status: "paid ? 'Paid' : 'Open'"
open:   "!closed"
flags:  "['urgent', 'late']"
```

Inside double quotes, YAML takes a backslash for itself, so write each one twice there. A pattern is easiest in an
expression YAML needs no quotes for, written as raw text: `regex.extract(value, r'\d+')`.

## Reading a message

A message's value is usually JSON. An expression reads its fields by name. The examples on this page read this
message:

```json
{
  "id": "A-100",
  "status": "paid",
  "total": 250,
  "currency": "eur",
  "created_at": "2026-11-14 22:30:00",
  "placed": "2026-11-14T22:30:00Z",
  "cancelled_at": null,
  "customer": { "name": " Ada Lovelace ", "email": "Ada@Example.com" },
  "tags": ["gift", "rush"],
  "lines": [
    { "sku": "a", "qty": 2, "price": 5 },
    { "sku": "b", "qty": 0, "price": 7 },
    { "sku": "c", "qty": 4, "price": 3.625 }
  ]
}
```

| Example | Reads | Result |
|---|---|---|
| `id` | the field `id` | `"A-100"` |
| `customer.email` | the field `email` inside the field `customer` | `"Ada@Example.com"` |
| `lines[0].sku` | the field `sku` of the first item of the list `lines`, counting from 0 | `"a"` |
| `lines[size(lines) - 1].sku` | the last item's | `"c"` |
| `value['created_at']` | a field by its name in quotes, for one whose name is not a plain word, such as `value['line-items']` | `"2026-11-14 22:30:00"` |
| `discount` | a field the message does not have: `null`, not an error | `null` |
| `customer.phone` | the same, inside a field | `null` |

`value` is the whole message value. The message's details are under `message`:

| Expression | Is |
|---|---|
| `message.key` | the message key, as text |
| `message.headers['trace-id']` | a header, as text |
| `message.topic` | the topic it came from |
| `message.partition` | its partition |
| `message.offset` | its offset |
| `message.timestamp` | when it happened, as a time |

If the message itself has a field called `value` or `message`, write `value.value` or `value.message`. A pipeline's
`let` names and the `item` of `each` are read by name too.

## Values

| Kind | Written | Notes |
|---|---|---|
| number | `42`, `-7`, `3.14`, `1.5e3` | Every number is a decimal: `12.5 * 2` is `25`, and `total / size(lines)` is an average. Exact to about 15 significant digits, so `0.1 + 0.2` is `0.30000000000000004`: round money with `round(total, 2)`. A whole number of more than 15 digits, such as an identifier, is kept exactly as it came; calculate with it after `double(id)`. |
| text | `'hello'`, `"hello"` | `\'` is a quote, `\\` a backslash, `\n` a new line. `r'\d+'` is raw: backslashes are just backslashes. `'''...'''` runs over several lines. |
| true / false | `true`, `false` | |
| null | `null` | Nothing there. |
| list | `[1, 2, 3]` | |
| record | `{'name': 'Ada', 'zip code': '94105'}` | Field names are in quotes. |
| time | `timestamp('2026-11-15T09:52:31Z')` | An instant. Written into JSON as UTC text: `"2026-11-15T09:52:31Z"`. |
| duration | `duration('1h30m')` | A length of time: `h`, `m`, `s`, `ms`. Written into JSON as seconds: `"5400s"`. |

Text that holds a number is still text: `double('19.90')` makes it a number.

## Operators

From the loosest to the tightest:

| Operator | Meaning | Example | Result |
|---|---|---|---|
| `a ? b : c` | `b` when `a` is true, otherwise `c` | `total > 100 ? 'large' : 'small'` | `"large"` |
| `\|\|` | either is true | `status == 'paid' \|\| status == 'sent'` | `true` |
| `&&` | both are true | `total > 0 && size(lines) > 0` | `true` |
| `==` `!=` | equal, not equal | `currency != 'usd'` | `true` |
| `<` `<=` `>` `>=` | numbers by value, text alphabetically, times by time | `total >= 100` | `true` |
| `in` | in a list, or a field of a record | `'rush' in tags` | `true` |
| `+` | add numbers; join text, or lists; move a time on | `'INV-' + id` | `"INV-A-100"` |
| `-` | subtract; the time between two times | `total - 50` | `200` |
| `*` | multiply | `lines[0].qty * lines[0].price` | `10` |
| `/` | divide; dividing by zero is an error | `total / 8` | `31.25` |
| `%` | the remainder | `total % 7` | `5` |
| `!` | the opposite | `!(status == 'test')` | `true` |
| `-` | negative | `-total` | `-250` |

Parentheses group: `(a + b) * c`. `+` joins text only to text: `'INV-' + string(total)`.

## Null

`null` is what a field the message does not have reads as. Messages from real systems often leave fields out, so:

- **An operator or a function given `null` makes the whole answer `null`**, as in SQL: a missing quantity never
  quietly becomes a zero total, and a missing reading gives no temperature rather than an error.
- `x == null` tells whether a value is null or missing; `has(value.x)` whether a field is there at all.
- `where` keeps a message only when its answer is `true`: `null` drops it, like `false`.
- A value of the wrong kind is still an error, as it is a mistake in the data: `'two' * 2`.

| Example | Result |
|---|---|
| `cancelled_at == null` | `true` |
| `discount * 2` | `null` |
| `has(value.cancelled_at)` | `true` |
| `has(value.discount)` | `false` |
| `discount == null ? 0 : discount` | `0` |
| `[1, null, 2].sum()` | `3` |

## Working with lists

A function that goes through a list takes a name for each item and a rule written with it:

| Function | Gives | Example | Result |
|---|---|---|---|
| `size(list)` | how many items | `size(lines)` | `3` |
| `list.map(x, value)` | a new list, one value per item | `lines.map(l, l.sku)` | `["a", "b", "c"]` |
| `list.map(x, condition, value)` | one value per item that matches | `lines.map(l, l.qty > 0, l.sku)` | `["a", "c"]` |
| `list.filter(x, condition)` | the items that match | `size(lines.filter(l, l.qty > 0))` | `2` |
| `list.exists(x, condition)` | whether some item matches | `lines.exists(l, l.qty == 0)` | `true` |
| `list.all(x, condition)` | whether every item matches | `lines.all(l, l.price > 0)` | `true` |
| `list.exists_one(x, condition)` | whether exactly one item matches | `lines.exists_one(l, l.qty == 0)` | `true` |
| `list.sum()` | the total, nulls left out | `lines.map(l, l.qty * l.price).sum()` | `24.5` |
| `math.greatest(list)` | the largest | `math.greatest(lines.map(l, l.price))` | `7` |
| `math.least(list)` | the smallest | `math.least(lines.map(l, l.price))` | `3.625` |
| `value in list` | whether the list has the value | `'gift' in tags` | `true` |
| `list + list` | one list | `tags + ['new']` | `["gift", "rush", "new"]` |
| `list.join(separator)` | a list of text as one text | `tags.join(', ')` | `"gift, rush"` |
| `sets.contains(list, other)` | whether the list has every item of the other | `sets.contains(tags, ['rush'])` | `true` |
| `sets.intersects(list, other)` | whether they share an item | `sets.intersects(tags, ['rush', 'late'])` | `true` |

The name (`l`) is yours to choose. Inside the rule it is the item, and everything outside is still in reach:
`lines.filter(l, l.price > min_price)`.

## Functions

### Text

| Function | Gives | Example | Result |
|---|---|---|---|
| `text.trim()` | without spaces at either end | `customer.name.trim()` | `"Ada Lovelace"` |
| `text.lowerAscii()` | small letters (A to Z) | `customer.email.lowerAscii()` | `"ada@example.com"` |
| `text.upperAscii()` | capitals (a to z) | `currency.upperAscii()` | `"EUR"` |
| `size(text)` | how many characters | `size('café')` | `4` |
| `text.substring(start, end)` | part of the text, counting from 0, `end` left out | `created_at.substring(0, 10)` | `"2026-11-14"` |
| `text.substring(start)` | the rest of the text from `start` | `'ABC-123'.substring(4)` | `"123"` |
| `text.charAt(position)` | one character | `'ABC'.charAt(1)` | `"B"` |
| `text.indexOf(part)` | where the part first is, or -1 | `'a-b-c'.indexOf('-')` | `1` |
| `text.lastIndexOf(part)` | where the part last is, or -1 | `'a-b-c'.lastIndexOf('-')` | `3` |
| `text.replace(find, with)` | every `find` replaced | `'a-b-c'.replace('-', '/')` | `"a/b/c"` |
| `text.split(separator)` | a list of the pieces | `'a,b,c'.split(',')` | `["a", "b", "c"]` |
| `text.contains(part)` | whether the text has the part | `'hello world'.contains('wor')` | `true` |
| `text.startsWith(prefix)` | whether it starts with | `id.startsWith('A-')` | `true` |
| `text.endsWith(suffix)` | whether it ends with | `'report.csv'.endsWith('.csv')` | `true` |
| `text.matches(pattern)` | whether the pattern matches somewhere in it | `id.matches(r'^[A-Z]-\d+$')` | `true` |
| `template.format(list)` | each clause filled in turn | `'%s x %d'.format(['box', 3])` | `"box x 3"` |
| `string(value)` | the value as text | `'INV-' + string(total)` | `"INV-250"` |

Positions and sizes count characters (`'café'` has 4), not bytes. `format`'s clauses are `%s` (any value), `%d` (a
whole number), `%f` and `%e` (with places: `%.2f`), `%x` (hexadecimal), and `%%` for a percent sign.

### Patterns

| Function | Gives | Example | Result |
|---|---|---|---|
| `regex.extract(text, pattern)` | the first match, or the part in its one group; `null` if none | `regex.extract('order 1042 ok', r'\d+')` | `"1042"` |
| | | `regex.extract(created_at, r'^(\d{4})-')` | `"2026"` |
| `regex.extractAll(text, pattern)` | every match | `regex.extractAll('1, 22, 333', r'\d+')` | `["1", "22", "333"]` |
| `regex.replace(text, pattern, with)` | every match replaced; `\1` is the first group | `regex.replace('2026-11-15', r'(\d+)-(\d+)-(\d+)', r'\3/\2/\1')` | `"15/11/2026"` |
| `regex.replace(text, pattern, with, count)` | the first `count` matches replaced | `regex.replace('a-b-c', '-', '+', 1)` | `"a+b-c"` |

Patterns use the usual regular-expression syntax, without look-around and back-references, so they always run in
time proportional to the text: no pattern can make a pipeline hang.

### Numbers

| Function | Gives | Example | Result |
|---|---|---|---|
| `round(number)` | rounded half away from zero | `round(2.5)` | `3` |
| `round(number, places)` | rounded to that many places, as the number reads | `round(2.345, 2)` | `2.35` |
| `math.floor(number)` | rounded down | `math.floor(2.7)` | `2` |
| `math.ceil(number)` | rounded up | `math.ceil(2.1)` | `3` |
| `math.trunc(number)` | without its fraction | `math.trunc(-2.7)` | `-2` |
| `math.abs(number)` | without its sign | `math.abs(-4)` | `4` |
| `math.sign(number)` | -1, 0 or 1 | `math.sign(-4)` | `-1` |
| `math.greatest(a, b, ...)` | the largest | `math.greatest(3, 9, 5)` | `9` |
| `math.least(a, b, ...)` | the smallest | `math.least(3, 9, 5)` | `3` |
| `int(number)` | the whole part | `int(2.7)` | `2` |
| `'%.2f'.format([number])` | as text with exactly that many decimals | `'%.2f'.format([24.5])` | `"24.50"` |

### Deciding

| Function | Gives | Example | Result |
|---|---|---|---|
| `condition ? a : b` | `a` when true, otherwise `b` | `size(lines) > 2 ? 'big' : 'small'` | `"big"` |
| `has(value.field)` | whether the field is there, even if null | `has(customer.email)` | `true` |
| `type(value)` | its kind: `double` (a number), `string`, `bool`, `list`, `map` (a record), `null_type` | `type(total) == double` | `true` |
| `cel.bind(name, value, expression)` | the expression, with the value worked out once under the name | `cel.bind(t, total * 1.2, t > 250 ? t : 250)` | `300` |

A chain of conditions reads in order: `total > 500 ? 'large' : total > 100 ? 'medium' : 'small'`.

### Converting

| Function | Gives | Example | Result |
|---|---|---|---|
| `string(value)` | the value as text | `string(12.5)` | `"12.5"` |
| `double(text)` | the number the text holds | `double('19.90')` | `19.9` |
| `int(text)` | the whole number the text holds | `int('42')` | `42` |
| `bool(text)` | `true` or `false` | `bool('true')` | `true` |

Text that holds no number is an error for `double` and `int`.

### Time

Times are instants. A zone, where one is asked for, is a name such as `'UTC'`, `'Europe/Berlin'` or
`'America/New_York'`, and is UTC when left out. An expression has no clock of its own: a message's time is
`message.timestamp`.

| Function | Gives | Example | Result |
|---|---|---|---|
| `timestamp(text)` | the time, written as RFC 3339 | `timestamp('2026-11-15T09:52:31Z')` | `"2026-11-15T09:52:31Z"` |
| `timestamp(text, layout)` | the time, written another way | `timestamp(created_at, '%Y-%m-%d %H:%M:%S')` | `"2026-11-14T22:30:00Z"` |
| `timestamp(seconds)` | the time that many seconds after 1970 | `timestamp(1763200000)` | `"2025-11-15T09:46:40Z"` |
| `int(time)` | seconds since 1970 | `int(timestamp(placed))` | `1794695400` |
| `time.getFullYear(zone)` | the year | `timestamp(placed).getFullYear()` | `2026` |
| `time.getMonth(zone)` | the month, from 0 (January) to 11 | `timestamp(placed).getMonth()` | `10` |
| `time.getDate(zone)` | the day of the month, from 1 | `timestamp(placed).getDate()` | `14` |
| `time.getDayOfWeek(zone)` | the day of the week, from 0 (Sunday) to 6 | `timestamp(placed).getDayOfWeek()` | `6` |
| `time.getHours(zone)` | the hour, 0 to 23 | `timestamp(placed).getHours('Asia/Tokyo')` | `7` |
| `time.getMinutes(zone)` | the minute | `timestamp(placed).getMinutes()` | `30` |
| `time + duration` | the time moved on | `timestamp(placed) + duration('90m')` | `"2026-11-15T00:00:00Z"` |
| `time - time` | the time between | `timestamp('2026-11-15T00:00:00Z') - timestamp(placed)` | `"5400s"` |
| `duration.getMinutes()` | a duration in whole minutes | `duration('1h30m').getMinutes()` | `90` |
| `string(time)` | the time as RFC 3339 text | `string(timestamp(placed)).substring(0, 10)` | `"2026-11-14"` |

Every `get` takes a zone or none. A layout uses the common `%` codes: `%Y` year, `%m` month, `%d` day, `%H` hour
(24-hour clock), `%M` minute, `%S` second, `%z` offset (`+0100`), `%b` month name (`Nov`). A layout without a zone
reads the time as UTC; one without a time of day reads midnight.

### Encoding

| Function | Gives | Example | Result |
|---|---|---|---|
| `base64.encode(bytes(text))` | text in Base64 | `base64.encode(bytes('hi'))` | `"aGk="` |
| `string(base64.decode(text))` | text out of Base64 | `string(base64.decode('aGk='))` | `"hi"` |
| `json.decode(text)` | the value JSON text holds: a field that carries JSON | `json.decode('{"a": [1, 2]}').a[1]` | `2` |
| `sha256(text)` | the SHA-256 digest, as hex: to stand in for personal data | `sha256('abc')` | `"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"` |

## When an expression cannot answer

An expression that cannot work for one message stops for that message alone, with a reason: text where a number
is needed, a division by zero, a pattern that is not valid, a time it cannot read. The pipeline then does what its
`on_error` setting says ([Pipelines](pipelines.md#when-a-message-fails)). An expression that could work for no
message, such as a misspelled function or a missing bracket, is refused when the pipeline starts, with the place
it went wrong:

```text
pipeline orders-clean: map.total: there is no function "summ"
pipeline orders-clean: map.tax: the expression ends too soon, at character 8:
  total *
         ^
```

## Limits

So that no message and no expression can take over the broker:

- An expression is at most 4,096 bytes long, with brackets nested at most 32 deep and at most 256 operators: a
  longer calculation is clearer as several `let` names anyway.
- Lists are gone through at most three deep, one inside another, and at most 100,000 steps in all.
- An expression has 100 milliseconds per message; its answer, and any text it makes, is at most 1 MiB.
- A pattern compiles to at most 1 MiB, and always runs in time proportional to the text.
- An expression has no loops of its own, no recursion, and no access to files, the network or other messages.

## Next to CEL elsewhere

OpenDuct runs CEL's standard functions with its math, sets, encoders and bindings extensions, the text functions of
its strings extension, and its regex extension. Its own rules, for business data, are where this page says them:
every number is a decimal (standard CEL keeps whole numbers apart, and `1 + 2.5` would be an error), an operation
on null gives null, `regex.extract` gives null rather than an optional, and `round`, `sum`, `sha256` and the two
extra ways of writing `timestamp` are OpenDuct's.
