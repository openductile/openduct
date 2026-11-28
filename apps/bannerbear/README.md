# Bannerbear

Images, videos and PDFs made from [Bannerbear](https://www.bannerbear.com) templates - social cards, banners,
certificates, product pictures - with each layer's text, picture or color filled in from a workflow, and Bannerbear's
design tools (background removal, PDFs) and its own workflows.

Source: Bannerbear's V5 OpenAPI 3.1 description, <https://api.bannerbear.com/v5/openapi.json>, and its API reference,
<https://developers.bannerbear.com> (both fetched 2026-11-27). Neither states a licence, so every label and
description here is in our own words. Written by hand.

## Signing in

- **An API key** (`bannerbear`): a V5 key from <https://app.bannerbear.com/v5/api_keys> (`bb_ak_v5_...`), sent as a
  bearer token. A key can be limited to some resources (`images:write` and so on); one limited too far is refused
  with 403. Keys of Bannerbear's older Version 2 API do not work here.

## Actions

- **Make an image** (`create_image`): from a template, with `objects` - the layers to change, by name:
  `[{name: title, text: ...}, {name: photo, image_url: ...}, {name: badge, color: "#FF0000"}]`. It comes back
  `pending`; **Look up an image** (`get_image`) gives the files when `completed`.
- **Make an image and wait** (`create_image_and_wait`): the same on Bannerbear's synchronous address
  (`sync.api.bannerbear.com`), which answers with the finished file - within 10 seconds, or 408.
- **List images** (`list_images`), **List image templates** (`list_image_templates`), **Look up an image template**
  (`get_image_template`): the template's layers are the names `objects` takes.
- **Make many images** (`create_batch`, up to 100) and **Look up a batch** (`get_batch`).
- **Make a video** (`create_animation`) from an animation template, and **Look up a video** (`get_animation`).
- **List Bannerbear workflows** (`list_workflows`), **Run a Bannerbear workflow** (`run_workflow`), **Look up a
  workflow run** (`get_workflow_run`): workflows built in Bannerbear itself.
- **Remove a picture's background** (`remove_background`), **Join files into one PDF** (`create_pdf`), and **Look up
  a tool job** (`get_tool_job`) for their files.
- **Account and quota** (`get_account`).

Lists read up to 25 a page, every page up to their most (100 images, 200 templates or workflows).

## Not done

- A trigger: Bannerbear signs its webhook deliveries with a key, but its reference does not say how the signature is
  sent, so none is checked here. Follow a render with the Look up actions, or Make an image and wait.
- Bannerbear's other tools (video trimming, subtitles, voice-overs, AI images and text, music, slideshows), signed
  instant URLs, uploads of assets (multipart), the public library and recipes, editor sessions, and changing
  templates and webhooks.

## Tested

Recorded exchanges (`tests/`) for every action, a refusal and the synchronous timeout - made-up values (and made-up
refusal messages) in the shapes Bannerbear's API description gives. Not run against a live Bannerbear account.

```sh
openduct app check apps/bannerbear --bar 80
openduct app test apps/bannerbear
```
