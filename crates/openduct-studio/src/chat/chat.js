// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0
//
// A workflow's chat page: each message is sent to the workflow, and its answer shown as it is made. Words are shown
// as text, never as HTML; nothing leaves the page but the messages, to the page's own address, with no cookie. The
// conversation's id is kept in this browser, so a conversation goes on after the page is opened again.
(function () {
  "use strict";
  var root = document.getElementById("chat");
  if (!root) return;
  var data = root.dataset;
  var endpoint = data.endpoint;
  var key = "openduct-chat:" + data.hook;
  var errorText = data.error || "Sorry, something went wrong. Please try again.";
  var max = parseInt(data.max, 10) || 4000;
  var list = document.getElementById("messages");
  var ask = document.getElementById("ask");
  var text = document.getElementById("text");
  var send = ask.querySelector("button");
  var gate = document.getElementById("gate");
  var passwordBox = document.getElementById("password");
  var gateNote = document.getElementById("gate-note");

  function stored(store, name) {
    try {
      return window[store].getItem(key + ":" + name);
    } catch (e) {
      return null;
    }
  }
  function keep(store, name, value) {
    try {
      if (value === null) window[store].removeItem(key + ":" + name);
      else window[store].setItem(key + ":" + name, value);
    } catch (e) {
      // Not kept: it lasts as long as this visit.
    }
  }

  function newSession() {
    var bytes = new Uint8Array(16);
    crypto.getRandomValues(bytes);
    var id = "";
    for (var i = 0; i < bytes.length; i++) id += (bytes[i] + 256).toString(16).slice(1);
    keep("localStorage", "session", id);
    return id;
  }

  var session = stored("localStorage", "session") || newSession();
  var password = stored("sessionStorage", "password") || "";
  var person = {};
  var query = new URLSearchParams(location.search);
  ["name", "email"].forEach(function (k) {
    var v = query.get(k);
    if (v) person[k] = v.slice(0, 200);
  });

  var shown = [];
  try {
    shown = JSON.parse(stored("localStorage", "shown") || "[]");
  } catch (e) {
    shown = [];
  }
  if (!Array.isArray(shown)) shown = [];

  function bubble(who, words) {
    var item = document.createElement("li");
    item.className = who;
    item.textContent = words;
    list.appendChild(item);
    list.scrollTop = list.scrollHeight;
    return item;
  }
  function remember(who, words) {
    shown.push({ who: who, text: words });
    shown = shown.slice(-100);
    keep("localStorage", "shown", JSON.stringify(shown));
  }
  shown.forEach(function (m) {
    if (m && typeof m.text === "string") bubble(m.who === "me" ? "me" : "bot", m.text);
  });

  function busy(on) {
    send.disabled = on;
    text.disabled = on;
    if (!on) text.focus();
  }
  function fail(item, words) {
    item.textContent = words;
    item.className = "bot failed";
  }

  function askPassword(note) {
    gate.hidden = false;
    ask.hidden = true;
    gateNote.textContent = note || "";
    passwordBox.focus();
  }
  if (data.password === "1" && !password) askPassword("");
  gate.addEventListener("submit", function (e) {
    e.preventDefault();
    password = passwordBox.value;
    keep("sessionStorage", "password", password);
    gate.hidden = true;
    ask.hidden = false;
    text.focus();
  });

  document.getElementById("new").addEventListener("click", function () {
    session = newSession();
    shown = [];
    keep("localStorage", "shown", null);
    while (list.children.length > 1) list.removeChild(list.lastChild);
    text.focus();
  });
  var close = document.getElementById("close");
  if (close) {
    close.addEventListener("click", function () {
      window.parent.postMessage({ openductChat: "close" }, "*");
    });
  }

  text.addEventListener("keydown", function (e) {
    if (e.key === "Enter" && !e.shiftKey && !e.isComposing) {
      e.preventDefault();
      if (ask.requestSubmit) ask.requestSubmit();
      else ask.dispatchEvent(new Event("submit", { cancelable: true }));
    }
  });
  text.addEventListener("input", function () {
    text.style.height = "auto";
    text.style.height = Math.min(text.scrollHeight, 160) + "px";
  });

  // The answer, as events: `text` as it is made, `reset` when its step starts again, then `answer` or `error`.
  function read(response, item) {
    var said = "";
    var final = null;
    var failure = null;
    var buffer = "";
    function onEvent(name, payload) {
      var d;
      try {
        d = JSON.parse(payload);
      } catch (e) {
        return;
      }
      if (name === "text" && typeof d.text === "string") {
        said += d.text;
        item.textContent = said;
        item.className = "bot";
        list.scrollTop = list.scrollHeight;
      } else if (name === "reset") {
        said = "";
        item.textContent = "…";
        item.className = "bot pending";
      } else if (name === "answer") {
        final = typeof d.answer === "string" ? d.answer : "";
      } else if (name === "error") {
        failure = typeof d.error === "string" ? d.error : errorText;
      }
    }
    function parse(chunk) {
      buffer += chunk.replace(/\r\n/g, "\n");
      var end;
      while ((end = buffer.indexOf("\n\n")) >= 0) {
        var block = buffer.slice(0, end);
        buffer = buffer.slice(end + 2);
        var name = "message";
        var lines = [];
        block.split("\n").forEach(function (line) {
          if (line.indexOf("event:") === 0) name = line.slice(6).trim();
          else if (line.indexOf("data:") === 0) lines.push(line.slice(5).replace(/^ /, ""));
        });
        if (lines.length) onEvent(name, lines.join("\n"));
      }
    }
    function done() {
      if (failure !== null) return fail(item, failure);
      if (final === null) {
        // The answer was cut off on its way.
        if (!said) return fail(item, errorText);
        final = said;
      }
      if (!final) {
        item.remove();
        return;
      }
      item.textContent = final;
      item.className = "bot";
      remember("bot", final);
    }
    if ((response.headers.get("content-type") || "").indexOf("json") >= 0) {
      return response.json().then(function (d) {
        final = typeof d.answer === "string" ? d.answer : "";
        done();
      });
    }
    if (!response.body || !response.body.getReader) {
      return response.text().then(function (t) {
        parse(t + "\n\n");
        done();
      });
    }
    var reader = response.body.getReader();
    var decoder = new TextDecoder();
    function pump() {
      return reader.read().then(function (r) {
        if (r.done) {
          parse(decoder.decode() + "\n\n");
          return done();
        }
        parse(decoder.decode(r.value, { stream: true }));
        return pump();
      });
    }
    return pump();
  }

  ask.addEventListener("submit", function (e) {
    e.preventDefault();
    var words = text.value.trim();
    if (!words || send.disabled) return;
    if (words.length > max) words = words.slice(0, max);
    text.value = "";
    text.style.height = "auto";
    var mine = bubble("me", words);
    remember("me", words);
    var item = bubble("bot pending", "…");
    busy(true);
    var body = { message: words, session: session, person: person };
    if (password) body.password = password;
    fetch(endpoint, {
      method: "POST",
      headers: { "content-type": "application/json", accept: "text/event-stream" },
      body: JSON.stringify(body),
      credentials: "omit",
      cache: "no-store",
    })
      .then(function (response) {
        if (response.status === 401) {
          password = "";
          keep("sessionStorage", "password", null);
          mine.remove();
          item.remove();
          shown.pop();
          keep("localStorage", "shown", JSON.stringify(shown));
          text.value = words;
          askPassword("The password is missing or wrong.");
          return;
        }
        if (!response.ok) {
          return response
            .json()
            .catch(function () {
              return {};
            })
            .then(function (said) {
              fail(item, typeof said.error === "string" ? said.error : errorText);
            });
        }
        return read(response, item);
      })
      .catch(function () {
        fail(item, errorText);
      })
      .then(function () {
        busy(false);
      });
  });

  text.focus();
})();
