// Compile-only tests: preserve real port signatures instead of erasing them to Function.
import { createCodexStorage } from "../src/infrastructure/codex-storage.js";

const storage = createCodexStorage({
  config: { read: (key: string) => key.length },
  sessions: { count: () => 1 },
  stateDb: { update: (provider: string) => provider },
  globalState: { roots: () => ["fixture"] }
});
const size: number = storage.config.read("model_provider");
void size;
// @ts-expect-error Missing methods must not silently become arbitrary Functions.
storage.config.nonexistent();
// @ts-expect-error The input parameter type must survive composition.
storage.stateDb.update(42);
// @ts-expect-error Ports must remain readonly.
storage.sessions.count = () => 2;
