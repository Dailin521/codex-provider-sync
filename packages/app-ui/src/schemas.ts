import { z } from "zod";

export const keepCountSchema = z.number().int().min(1).max(1000);

export const syncSchema = z.object({
  keepCount: keepCountSchema,
  syncMode: z.enum(["full", "fast"])
});

export const switchSchema = z.object({
  provider: z.string().trim().min(1).max(200).regex(/^[A-Za-z0-9._-]+$/),
  modelMode: z.enum(["provider-default", "keep-root-model", "explicit"]),
  model: z.string().trim().max(500).optional(),
  keepCount: keepCountSchema,
  syncMode: z.enum(["full", "fast"])
}).superRefine((value, context) => {
  if (value.modelMode === "explicit" && !value.model) {
    context.addIssue({ code: "custom", path: ["model"], message: "model-required" });
  }
  if (value.modelMode !== "explicit" && value.model) {
    context.addIssue({ code: "custom", path: ["model"], message: "model-not-accepted" });
  }
  if (value.syncMode === "fast" && value.modelMode !== "keep-root-model") {
    context.addIssue({ code: "custom", path: ["modelMode"], message: "fast-keeps-model" });
  }
});

export const restoreSchema = z.object({
  backupId: z.string().trim().min(1).max(300),
  restoreConfig: z.boolean(),
  restoreDatabase: z.boolean(),
  restoreSessions: z.boolean(),
  allowSqliteHomeRelocation: z.boolean(),
  relocationTargetProfileId: z.string().trim().max(80).optional()
}).superRefine((value, context) => {
  if (!value.restoreConfig && !value.restoreDatabase && !value.restoreSessions) {
    context.addIssue({ code: "custom", path: ["restoreSessions"], message: "restore-required" });
  }
  if (value.allowSqliteHomeRelocation && (!value.relocationTargetProfileId || value.restoreConfig)) {
    context.addIssue({ code: "custom", path: ["relocationTargetProfileId"], message: "relocation-invalid" });
  }
});

const absolutePath = z.string().trim().min(1).max(4096).refine(
  (value) => /^(?:[A-Za-z]:[\\/]|\\\\|\/)/.test(value),
  "absolute-path-required"
);

export const profileSchema = z.object({
  profileId: z.string().trim().min(1).max(80).regex(/^[A-Za-z0-9._-]+$/),
  name: z.string().trim().min(1).max(120),
  codexHome: absolutePath,
  sqliteHome: z.union([absolutePath, z.literal("")]).optional()
});
