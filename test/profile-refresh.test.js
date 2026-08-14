import assert from "node:assert/strict";
import test from "node:test";

import { createProfileRefresh, storagePayload } from "../web/src/profile-refresh.js";

function createDeferred() {
  let resolve;
  let reject;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function createRecordingFetches() {
  const calls = [];
  const pending = new Map();
  const fetchFor = (kind) => (storage, options = {}) => {
    const deferred = createDeferred();
    const key = `${kind}:${storage.profileId}`;
    calls.push({ kind, profileId: storage.profileId, signal: options.signal });
    pending.set(key, deferred);
    return deferred.promise;
  };
  return {
    calls,
    pending,
    fetchStatus: fetchFor("status"),
    fetchBackups: fetchFor("backups"),
    resolveFor(profileId, { status = {}, backups = {} } = {}) {
      pending.get(`status:${profileId}`)?.resolve({ status });
      pending.get(`backups:${profileId}`)?.resolve(backups);
    },
    rejectFor(profileId, error) {
      pending.get(`status:${profileId}`)?.reject(error);
      pending.get(`backups:${profileId}`)?.reject(error);
    }
  };
}

function createUiRecorder() {
  const recorder = {
    applied: [],
    errors: [],
    loading: []
  };
  recorder.onResult = (result) => recorder.applied.push(result);
  recorder.onError = (error) => recorder.errors.push(error.message);
  recorder.onLoading = (value) => recorder.loading.push(value);
  return recorder;
}

test("storagePayload defaults to the default profile", () => {
  assert.deepEqual(storagePayload(), { profileId: "default" });
  assert.deepEqual(storagePayload("work"), { profileId: "work" });
});

test("profile refresh race: A starts first, B finishes first, A finishes last — UI stays on B", async () => {
  const fetches = createRecordingFetches();
  const ui = createUiRecorder();
  const refresh = createProfileRefresh({ fetchStatus: fetches.fetchStatus, fetchBackups: fetches.fetchBackups });

  // The user is on profile A and a refresh is in flight.
  const refreshA = refresh({ profileId: "a", onLoading: ui.onLoading, onResult: ui.onResult, onError: ui.onError });
  // The user switches to profile B before A completes.
  const refreshB = refresh({ profileId: "b", onLoading: ui.onLoading, onResult: ui.onResult, onError: ui.onError });

  // Starting B aborts A's underlying requests.
  const requestA = fetches.calls.find((call) => call.profileId === "a");
  assert.equal(requestA.signal.aborted, true);

  // B finishes first and is applied.
  fetches.resolveFor("b", { status: { currentProvider: "provider-b" }, backups: { backups: ["b-backup"] } });
  assert.equal(await refreshB, true);
  assert.deepEqual(ui.applied.map((entry) => entry.profileId), ["b"]);
  assert.deepEqual(ui.loading, [true, true, false]);

  // A finishes last; its results, loading transitions, and errors must be discarded.
  fetches.resolveFor("a", { status: { currentProvider: "provider-a" }, backups: { backups: ["a-backup"] } });
  assert.equal(await refreshA, false);
  assert.deepEqual(ui.applied.map((entry) => entry.profileId), ["b"]);
  assert.deepEqual(ui.applied[0].status, { currentProvider: "provider-b" });
  assert.deepEqual(ui.errors, []);
  assert.deepEqual(ui.loading, [true, true, false]);
});

test("profile refresh race: a stale request failing late never surfaces an error", async () => {
  const fetches = createRecordingFetches();
  const ui = createUiRecorder();
  const refresh = createProfileRefresh({ fetchStatus: fetches.fetchStatus, fetchBackups: fetches.fetchBackups });

  const refreshA = refresh({ profileId: "a", onLoading: ui.onLoading, onResult: ui.onResult, onError: ui.onError });
  const refreshB = refresh({ profileId: "b", onLoading: ui.onLoading, onResult: ui.onResult, onError: ui.onError });

  fetches.resolveFor("b", { status: { currentProvider: "provider-b" } });
  assert.equal(await refreshB, true);

  fetches.rejectFor("a", new Error("profile A backend exploded"));
  assert.equal(await refreshA, false);
  assert.deepEqual(ui.errors, []);
  assert.deepEqual(ui.applied.map((entry) => entry.profileId), ["b"]);
});

test("profile refresh surfaces errors from the latest request only", async () => {
  const fetches = createRecordingFetches();
  const ui = createUiRecorder();
  const refresh = createProfileRefresh({ fetchStatus: fetches.fetchStatus, fetchBackups: fetches.fetchBackups });

  const refreshA = refresh({ profileId: "a", onLoading: ui.onLoading, onResult: ui.onResult, onError: ui.onError });
  fetches.rejectFor("a", new Error("profile A is unreachable"));
  assert.equal(await refreshA, false);
  assert.deepEqual(ui.errors, ["profile A is unreachable"]);
  assert.deepEqual(ui.applied, []);
  assert.deepEqual(ui.loading, [true, false]);
});

test("profile refresh quiet mode leaves the loading indicator untouched unless it is the latest", async () => {
  const fetches = createRecordingFetches();
  const ui = createUiRecorder();
  const refresh = createProfileRefresh({ fetchStatus: fetches.fetchStatus, fetchBackups: fetches.fetchBackups });

  const refreshA = refresh({ profileId: "a", onLoading: ui.onLoading, onResult: ui.onResult, onError: ui.onError });
  const quietB = refresh({ profileId: "b", showLoading: false, onLoading: ui.onLoading, onResult: ui.onResult, onError: ui.onError });

  fetches.resolveFor("b", { status: { currentProvider: "provider-b" } });
  assert.equal(await quietB, true);
  // The quiet refresh never raised the indicator but still settles it as the latest request.
  assert.deepEqual(ui.loading, [true, false]);

  fetches.resolveFor("a", { status: { currentProvider: "provider-a" } });
  assert.equal(await refreshA, false);
  assert.deepEqual(ui.loading, [true, false]);
});
