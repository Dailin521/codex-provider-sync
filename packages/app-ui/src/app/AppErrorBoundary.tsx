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
          <h1 className="mt-4 text-xl font-bold">{chinese ? "应用错误" : "Application error"}</h1>
          <p className="mt-2 text-sm text-[var(--muted)]">
            {chinese
              ? "页面遇到未预期错误；系统没有自动启动任何写操作。"
              : "The page encountered an unexpected error. No write was started automatically."}
          </p>
          <Button className="mt-5" onClick={() => globalThis.location?.reload()} type="button">
            {chinese ? "重新加载" : "Reload"}
          </Button>
        </Card>
      </div>
    );
  }
}
