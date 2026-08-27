import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig } from "electron-vite";

const root = path.dirname(fileURLToPath(import.meta.url));
const desktopBuildId = process.env.CPS_DESKTOP_BUILD_ID?.trim() || "dev-c9";

if (!/^[A-Za-z0-9._-]{1,128}$/.test(desktopBuildId)) {
  throw new Error("CPS_DESKTOP_BUILD_ID must be a 1-128 character safe identifier.");
}

export default defineConfig(({ mode }) => ({
  main: {
    define: {
      __CPS_DESKTOP_TEST_BUILD__: JSON.stringify(mode === "test"),
      __CPS_DESKTOP_FORCE_BETTER_SQLITE3__: JSON.stringify(mode === "test"),
      __CPS_DESKTOP_BUILD_ID__: JSON.stringify(desktopBuildId)
    },
    ssr: {
      noExternal: [/^@codex-provider-sync\//]
    },
    build: {
      outDir: path.resolve(root, "out/main"),
      sourcemap: false,
      externalizeDeps: {
        exclude: [
          "@codex-provider-sync/contracts",
          "@codex-provider-sync/core",
          "@codex-provider-sync/core-client"
        ]
      },
      rollupOptions: {
        input: {
          index: path.resolve(root, "src/main/index.ts"),
          runtime: path.resolve(root, "src/runtime/index.ts")
        },
        external: ["better-sqlite3"]
      }
    }
  },
  preload: {
    define: {
      __CPS_DESKTOP_TEST_BUILD__: JSON.stringify(mode === "test"),
      __CPS_DESKTOP_FORCE_BETTER_SQLITE3__: "false",
      __CPS_DESKTOP_BUILD_ID__: JSON.stringify(desktopBuildId)
    },
    ssr: {
      noExternal: [/^@codex-provider-sync\//]
    },
    build: {
      outDir: path.resolve(root, "out/preload"),
      sourcemap: false,
      externalizeDeps: {
        exclude: [
          "@codex-provider-sync/contracts",
          "@codex-provider-sync/core-client"
        ]
      },
      rollupOptions: {
        input: {
          index: path.resolve(root, "src/preload/index.ts")
        },
        external: ["electron"],
        output: {
          format: "cjs",
          entryFileNames: "[name].cjs",
          inlineDynamicImports: true
        }
      }
    }
  },
  renderer: {
    root: path.resolve(root, "src/renderer"),
    base: "./",
    plugins: [react(), tailwindcss()],
    build: {
      outDir: path.resolve(root, "out/renderer"),
      emptyOutDir: true,
      sourcemap: false,
      target: "es2022"
    }
  }
}));
