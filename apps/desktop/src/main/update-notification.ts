import type { MessageBoxOptions } from "electron";

/** Closing the native notice is not consent to download, install or mute. */
export async function showUpdateNotification(options: {
  version: string;
  chinese: boolean;
  show(input: MessageBoxOptions): Promise<{ response: number }>;
  ignore(version: string): Promise<void>;
}): Promise<void> {
  const { chinese, version } = options;
  const result = await options.show({
    type: "info",
    title: chinese ? "发现新版本" : "Update available",
    message: chinese ? `Codex Provider Sync ${version} 已可用` : `Codex Provider Sync ${version} is available`,
    detail: chinese ? "请在“设置 → 更新”中下载更新。" : "Download it in Settings → Updates.",
    buttons: [chinese ? "稍后" : "Later", chinese ? "不再提醒此版本" : "Don't remind me about this version"],
    defaultId: 0,
    cancelId: 0,
    noLink: true
  });
  if (result.response === 1) {
    try { await options.ignore(version); }
    catch {
      await options.show({
        type: "warning",
        message: chinese ? "未能保存提醒设置，请在“设置 → 更新”中重试。" : "Could not save your preference. Try again in Settings → Updates.",
        buttons: [chinese ? "知道了" : "OK"],
        noLink: true
      });
    }
  }
}
