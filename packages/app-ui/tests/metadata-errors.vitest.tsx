import { CoreClientError } from "@codex-provider-sync/core-client";
import { createPublicCoreErrorDto } from "@codex-provider-sync/contracts";
import { describe, expect, it } from "vitest";
import { createAppI18n } from "../src/i18n.js";
import { safeErrorText } from "../src/shared/presentation.js";

describe("metadata failures", () => {
  it.each(["en", "zh-CN"] as const)("shows distinct actionable errors in %s through CoreClient", async locale => {
    const i18n = await createAppI18n(locale);
    const texts = (["ROLLOUT_METADATA_TOO_LARGE", "ROLLOUT_METADATA_INVALID", "ROLLOUT_CHANGED"] as const).map(code => {
      const dto = createPublicCoreErrorDto(code);
      return safeErrorText(new CoreClientError(dto), i18n.t.bind(i18n));
    });
    expect(new Set(texts).size).toBe(3);
    expect(texts[0]).toContain("128 MiB");
    expect(texts[1]).toMatch(/元数据|session metadata/);
    expect(texts.every(text => text !== i18n.t("errors.fallback"))).toBe(true);
  });
});
