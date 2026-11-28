# QuickChart

Charts and QR codes from [QuickChart](https://quickchart.io), as links to images a workflow can put in an email, a
chat message or a report - and QR codes read back from a picture.

Source: QuickChart's own OpenAPI 3.1 description, <https://quickchart.io/openapi.json> (QuickChart API 1.0.0,
fetched 2026-11-27), and its documentation, <https://quickchart.io/documentation/>. The description states no
licence, so every label and description here is in our own words. Written by hand.

## Signing in

- **QuickChart, free or with an API key** (`quickchart`): QuickChart draws charts without an account, within its
  free limits; leave the key empty for that. A paid plan's key (from the QuickChart dashboard) raises the limits and
  keeps chart links for 6 months instead of 3 days; it is sent as the `key` parameter (and, for a chart link, in the
  body too). A credential is still made for the app, even an empty one.

## Actions

- **Make a chart link** (`create_chart_link`): a [Chart.js](https://www.chartjs.org) chart - `{type: bar, data:
  {labels: [...], datasets: [{label: ..., data: [...]}]}}`, as a record or as text - kept, and a short `url` to its
  image (PNG by default; JPG, WebP, SVG or PDF as asked).
- **Draw a chart as Base64** (`chart_image`): the image now, as Base64 text in `image_base64`.
- **Check a chart** (`check_chart`): whether it can be drawn, with `errors` and `warnings`; a chart that cannot fails
  the step with QuickChart's errors.
- **Make a chart from a description** (`chart_from_words`): a sentence with the numbers in it becomes a chart: its
  configuration, a link to its image, and a link to change it in QuickChart's chart maker.
- **Make a QR code link** (`qr_code_link`), **Make many QR code links** (`qr_code_links`): links to QR code images
  for text or addresses, with size, colors, a caption or a logo in the middle.
- **Read a QR code** (`read_qr_code`): what a QR code in a public image says.

## Not done

- Images as files: a chart, word cloud, table, barcode, Graphviz diagram or watermark drawn as an image file (the
  answer is the picture itself, which an app action does not yet keep as a run's file); the ZIP of many QR codes.
- Google Image Charts requests, saved templates and the QR code checks without drawing.

## Tested

Recorded exchanges (`tests/`) for every action, with and without a key, and a chart refused in QuickChart's words -
made-up values, shaped as QuickChart's API description says. Not run against QuickChart.

```sh
openduct app check apps/quickchart --bar 80
openduct app test apps/quickchart
```
