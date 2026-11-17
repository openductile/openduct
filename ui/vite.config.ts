// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// `npm run dev` serves the page with live reloading and sends the API's requests to an `openduct start` on this
// machine; `npm run build` writes dist/, which building OpenDuct puts inside the program.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5481,
    proxy: { "/api": "http://127.0.0.1:5480", "/health": "http://127.0.0.1:5480" },
  },
  build: { outDir: "dist", emptyOutDir: true, sourcemap: false },
});
