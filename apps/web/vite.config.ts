import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig } from "vite";

const root = path.dirname(fileURLToPath(import.meta.url));

export default defineConfig({
  root,
  plugins: [react(), tailwindcss()],
  build: {
    outDir: path.resolve(root, "../../web/dist"),
    emptyOutDir: true,
    sourcemap: false,
    target: "es2022"
  },
  server: { host: "127.0.0.1", port: 5173 }
});
