import { ShieldAlert } from "lucide-react";
import { Component, type ErrorInfo, type ReactNode } from "react";

import { Button, Card } from "../ui.js";

export class AppErrorBoundary extends Component<{
  children: ReactNode;
  locale(): string;
}, { failed: boolean }> {
  state = { failed: false };

  static getDerivedStateFromError(): { failed: boolean } {
    return { failed: true };
  }

  componentDidCatch(_error: Error, _info: ErrorInfo): void {}

  render(): ReactNode {
    if (!this.state.failed) return this.props.children;
    const chinese = this.props.locale().toLowerCase().startsWith("zh");
    return (
      <div className="grid min-h-screen place-items-center bg-[var(--surface)] p-6 text-[var(--text)]">
        <Card className="max-w-lg text-center">
          <ShieldAlert className="mx-auto text-[var(--danger)]" size={40} />
          <h1 className="mt-4 text-xl font-bold">{chinese ? "页面暂时无法显示" : "This page is temporarily unavailable"}</h1>
          <p className="mt-2 text-sm text-[var(--muted)]">
            {chinese
              ? "你的数据没有被更改。请重新打开应用；如果问题持续，请查看操作日志或导出诊断信息。"
              : "Your data was not changed. Reopen the app; if the problem continues, check Operation logs or export diagnostics."}
          </p>
          <Button className="mt-5" onClick={() => globalThis.location?.reload()} type="button">
            {chinese ? "重新打开" : "Reopen"}
          </Button>
        </Card>
      </div>
    );
  }
}
