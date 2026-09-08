import {
  createCoreOperationStartedEnvelope,
  createCoreProgressEnvelope,
  type PlanSummary
} from "@codex-provider-sync/contracts";
import { MockCoreClient } from "@codex-provider-sync/core-client";
import { act, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { AppUi } from "../src/App.js";
import { statusFor, syncPlanFor } from "./helpers/app-fixtures.js";

const preferences = {
  getLocale: () => "en" as const,
  setLocale: vi.fn(),
  getTheme: () => "system" as const,
  setTheme: vi.fn()
};

describe("App operation lifecycle", () => {
  it("keeps a locally edited repair preview visible but prevents applying it after a failed re-prepare", async () => {
    const repairPlan = (planId: string): PlanSummary => ({
      ...syncPlanFor("profile-r1", planId),
      operation: "repair",
      target: { provider: "openai", targets: ["models"], scope: "all" },
      impact: {
        rolloutFilesToChange: 1,
        sqliteRowsToChange: 1,
        backupExpected: true,
        repairPreview: [{ sessionId: "session-1", changes: [{ target: "models", before: "old-model", after: "new-model" }] }],
        repairPreviewTotal: 1,
        repairPreviewTruncated: false
      }
    } as PlanSummary);
    const dismissOperationPlan = vi.fn(async () => {});
    const prepareRepair = vi.fn()
      .mockResolvedValueOnce(repairPlan("repair-old"))
      .mockRejectedValueOnce({ code: "INTERNAL_ERROR" });
    const applyRepair = vi.fn();
    const user = userEvent.setup();
    render(<AppUi surface="desktop" core={new MockCoreClient({ getStatus: async () => statusFor(), prepareRepair, applyRepair })} host={{ dismissOperationPlan, listProfiles: async () => [{ id: "default", name: "Default", revision: "profile-r1" }] }} initialLocale="en" initialTheme="system" preferences={preferences} />);

    await screen.findByRole("button", { name: "Advanced features", exact: true });
    await user.click(screen.getByRole("button", { name: "Advanced features", exact: true }));
    await user.click(screen.getByText("Advanced adjustments", { exact: true }));
    await user.click(screen.getByRole("checkbox", { name: "Unify historical model names" }));
    await user.click(screen.getByRole("button", { name: "Preview adjustment" }));
    await screen.findByRole("dialog", { name: "Confirm repair" });

    await user.click(screen.getByRole("radio", { name: "Selected chats" }));
    await user.click(screen.getByRole("checkbox", { name: "Select chat session-1" }));
    expect(screen.getByRole("button", { name: "Confirm repair" })).toBeDisabled();
    await user.click(screen.getByRole("button", { name: "Update repair preview" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("The earlier preview has been dismissed");
    expect(screen.getByRole("dialog", { name: "Confirm repair" })).toBeVisible();
    expect(screen.getByRole("button", { name: "Confirm repair" })).toBeDisabled();
    expect(dismissOperationPlan).toHaveBeenCalledWith("repair-old");
    // Returning the draft to its old scope must not revive a dismissed plan.
    await user.click(screen.getByRole("radio", { name: "Whole profile" }));
    expect(screen.getByRole("button", { name: "Confirm repair" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Update repair preview" })).toBeEnabled();
    expect(applyRepair).not.toHaveBeenCalled();
  });

  it("refreshes a profile that changes while preparing and retries with the new revision", async () => {
    let profileRevision = "profile-r1";
    let prepareAttempts = 0;
    const preparedRevisions: Array<string | undefined> = [];
    const host = {
      listProfiles: vi.fn(async () => [{ id: "default", name: "Default", revision: profileRevision }])
    };
    const core = new MockCoreClient({
      getStatus: async ({ profile }) => statusFor(profile.profileRevision ?? profileRevision),
      prepareSync: async ({ profile }) => {
        preparedRevisions.push(profile.profileRevision);
        prepareAttempts += 1;
        if (prepareAttempts === 1) {
          profileRevision = "profile-r2";
          throw { code: "PROFILE_CHANGED" };
        }
        return syncPlanFor(profile.profileRevision, "plan-sync-retry");
      }
    });
    const user = userEvent.setup();

    render(
      <AppUi
        surface="desktop"
        core={core}
        host={host}
        initialLocale="en"
        initialTheme="system"
        preferences={preferences}
      />
    );

    await screen.findByRole("button", { name: "Preview sync" });
    const prepareButton = await screen.findByRole("button", { name: "Preview sync" });
    await waitFor(() => expect(prepareButton).toBeEnabled());
    await user.click(prepareButton);

    expect(await screen.findByText("Profile changed.")).toBeVisible();
    await waitFor(() => expect(host.listProfiles).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(prepareButton).toBeEnabled());
    await user.click(prepareButton);

    expect(await screen.findByRole("dialog", { name: "Confirm sync" })).toBeVisible();
    expect(preparedRevisions).toEqual(["profile-r1", "profile-r2"]);
  });

  it("recovers a Status poll after the selected profile revision changes externally", async () => {
    let profileRevision = "profile-r1";
    const statusRevisions: Array<string | undefined> = [];
    const host = {
      listProfiles: vi.fn(async () => [{ id: "default", name: "Default", revision: profileRevision }])
    };
    const core = new MockCoreClient({
      getStatus: async ({ profile }) => {
        statusRevisions.push(profile.profileRevision);
        if (profile.profileRevision === "profile-r1") {
          profileRevision = "profile-r2";
          throw { code: "PROFILE_CHANGED" };
        }
        return statusFor(profile.profileRevision);
      }
    });
    const user = userEvent.setup();

    render(
      <AppUi
        surface="desktop"
        core={core}
        host={host}
        initialLocale="en"
        initialTheme="system"
        preferences={preferences}
      />
    );

    expect(await screen.findByText("Profile changed.", {}, { timeout: 4000 })).toBeVisible();
    await waitFor(() => expect(host.listProfiles).toHaveBeenCalledTimes(2));
    await user.click(screen.getByRole("button", { name: "Overview", exact: true }));
    await waitFor(() => expect(screen.getByRole("button", { name: "Preview sync" })).toBeEnabled());
    expect(statusRevisions.at(-1)).toBe("profile-r2");
  });

  it("refreshes a changed profile, closes the stale plan, and prepares with the new revision", async () => {
    let profileRevision = "profile-r1";
    let planSequence = 0;
    const listedRevisions: string[] = [];
    const preparedRevisions: Array<string | undefined> = [];
    const host = {
      listProfiles: vi.fn(async () => {
        listedRevisions.push(profileRevision);
        return [{ id: "default", name: "Default", revision: profileRevision }];
      })
    };
    const core = new MockCoreClient({
      getStatus: async ({ profile }) => statusFor(profile.profileRevision ?? profileRevision),
      prepareSync: async ({ profile }): Promise<PlanSummary> => {
        preparedRevisions.push(profile.profileRevision);
        planSequence += 1;
        return syncPlanFor(profile.profileRevision, `plan-sync-${planSequence}`);
      },
      applySync: async () => {
        profileRevision = "profile-r2";
        throw { code: "STALE_STATE", details: { reason: "profile" } };
      }
    });
    const user = userEvent.setup();

    render(
      <AppUi
        surface="desktop"
        core={core}
        host={host}
        initialLocale="en"
        initialTheme="system"
        preferences={preferences}
      />
    );

    await screen.findByRole("button", { name: "Preview sync" });
    const prepareButton = await screen.findByRole("button", { name: "Preview sync" });
    await waitFor(() => expect(prepareButton).toBeEnabled());
    await user.click(prepareButton);
    await user.click(await screen.findByRole("button", { name: "Confirm sync" }));

    expect(await screen.findByText("Profile changed.")).toBeVisible();
    expect(screen.getByText("Review the selected storage location and try again.")).toBeVisible();
    await waitFor(() => expect(screen.queryByRole("dialog", { name: "Confirm sync" })).not.toBeInTheDocument());
    await waitFor(() => expect(prepareButton).toHaveFocus());
    expect(host.listProfiles).toHaveBeenCalledTimes(2);
    expect(listedRevisions).toEqual(["profile-r1", "profile-r2"]);

    await waitFor(() => expect(prepareButton).toBeEnabled());
    await user.click(prepareButton);
    expect(await screen.findByRole("dialog", { name: "Confirm sync" })).toBeVisible();
    expect(preparedRevisions).toEqual(["profile-r1", "profile-r2"]);
  });

  it("renders trusted progress and cancels through the active AbortSignal", async () => {
    const operationId = "11111111-1111-4111-8111-111111111111";
    let rejectApply!: (reason: unknown) => void;
    let applySignal: AbortSignal | undefined;
    const applyPending = new Promise<never>((_resolve, reject) => { rejectApply = reject; });
    const core = new MockCoreClient({
      getStatus: async () => statusFor(),
      prepareSync: async () => syncPlanFor(),
      applySync: async (_payload, request, control) => {
        applySignal = control.signal;
        control.onOperationStarted?.(createCoreOperationStartedEnvelope(request.requestId, operationId, "sync"));
        control.onProgress?.(createCoreProgressEnvelope(request.requestId, operationId, {
          stage: "create_backup",
          status: "progress",
          progress: 0.5,
          count: 1
        }));
        return applyPending;
      }
    });
    const user = userEvent.setup();

    render(
      <AppUi
        surface="desktop"
        core={core}
        host={{ listProfiles: async () => [{ id: "default", name: "Default", revision: "profile-r1" }] }}
        initialLocale="en"
        initialTheme="system"
        preferences={preferences}
      />
    );

    await screen.findByRole("button", { name: "Preview sync" });
    const prepareButton = await screen.findByRole("button", { name: "Preview sync" });
    await waitFor(() => expect(prepareButton).toBeEnabled());
    await user.click(prepareButton);
    await user.click(await screen.findByRole("button", { name: "Confirm sync" }));

    const dialog = await screen.findByRole("dialog", { name: "Confirm sync" });
    expect(within(dialog).queryByText(operationId)).not.toBeInTheDocument();
    expect(within(dialog).getByText("Create backup · In progress · 1")).toBeVisible();
    expect(within(dialog).getByRole("progressbar", { name: "Operation progress" })).toHaveValue(0.5);

    await user.click(within(dialog).getByRole("button", { name: "Cancel operation" }));
    expect(applySignal?.aborted).toBe(true);
    expect(within(dialog).getByRole("button", { name: "Cancelling…" })).toBeDisabled();
    expect(within(dialog).getByText("Cancellation will take effect at the next safe point.")).toBeVisible();

    await act(async () => {
      rejectApply({ code: "OPERATION_CANCELLED" });
      await applyPending.catch(() => undefined);
    });
    expect(await screen.findByText("Operation cancelled.")).toBeVisible();
    await waitFor(() => expect(screen.queryByRole("dialog", { name: "Confirm sync" })).not.toBeInTheDocument());
    expect(screen.queryByRole("dialog", { name: "Operation result" })).not.toBeInTheDocument();
    await waitFor(() => expect(prepareButton).toHaveFocus());
  });
});
