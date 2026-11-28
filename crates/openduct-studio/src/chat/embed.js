// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0
//
// A chat button for a site's pages: <script src="https://<studio>/chat/<hook>/embed.js" async></script>. The button
// opens the workflow's chat page in a frame, loaded the first time it is opened. `data-open="true"` opens it at once;
// `data-name` and `data-email` tell the workflow who the person is; `data-side="left"` puts it on the left.
(function () {
  "use strict";
  var script = document.currentScript;
  if (!script || !script.src) return;
  var config = __CONFIG__;
  var src = new URL(script.src, location.href);
  var base = src.origin + src.pathname.replace(/\/embed\.js$/, "");
  var side = script.getAttribute("data-side") === "left" ? "left" : "right";
  var query = new URLSearchParams({ embed: "1" });
  ["name", "email"].forEach(function (k) {
    var v = script.getAttribute("data-" + k);
    if (v) query.set(k, v.slice(0, 200));
  });

  var frame = document.createElement("iframe");
  frame.title = config.title;
  var f = frame.style;
  f.position = "fixed";
  f.bottom = "88px";
  f[side] = "20px";
  f.width = "min(380px, calc(100vw - 40px))";
  f.height = "min(560px, calc(100vh - 120px))";
  f.border = "0";
  f.borderRadius = "14px";
  f.boxShadow = "0 12px 40px rgba(0, 0, 0, 0.25)";
  f.background = config.background;
  f.zIndex = "2147483646";
  f.display = "none";

  var button = document.createElement("button");
  button.type = "button";
  button.setAttribute("aria-label", config.title);
  button.setAttribute("aria-expanded", "false");
  button.title = config.title;
  var b = button.style;
  b.position = "fixed";
  b.bottom = "20px";
  b[side] = "20px";
  b.width = "56px";
  b.height = "56px";
  b.border = "0";
  b.borderRadius = "50%";
  b.background = config.accent;
  b.color = config.onAccent;
  b.boxShadow = "0 6px 20px rgba(0, 0, 0, 0.25)";
  b.cursor = "pointer";
  b.zIndex = "2147483647";
  b.display = "flex";
  b.alignItems = "center";
  b.justifyContent = "center";
  var svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.setAttribute("viewBox", "0 0 24 24");
  svg.setAttribute("width", "26");
  svg.setAttribute("height", "26");
  svg.setAttribute("aria-hidden", "true");
  var path = document.createElementNS("http://www.w3.org/2000/svg", "path");
  path.setAttribute("d", "M4 5h16v11H9l-5 4z");
  path.setAttribute("fill", "none");
  path.setAttribute("stroke", "currentColor");
  path.setAttribute("stroke-width", "2");
  path.setAttribute("stroke-linejoin", "round");
  svg.appendChild(path);
  button.appendChild(svg);

  var open = false;
  function show(on) {
    open = on;
    if (on && !frame.src) frame.src = base + "?" + query.toString();
    f.display = on ? "block" : "none";
    button.setAttribute("aria-expanded", on ? "true" : "false");
  }
  button.addEventListener("click", function () {
    show(!open);
  });
  window.addEventListener("message", function (e) {
    if (e.source === frame.contentWindow && e.origin === src.origin && e.data && e.data.openductChat === "close") {
      show(false);
      button.focus();
    }
  });

  function place() {
    document.body.appendChild(frame);
    document.body.appendChild(button);
    if (script.getAttribute("data-open") === "true") show(true);
  }
  if (document.body) place();
  else document.addEventListener("DOMContentLoaded", place);
})();
