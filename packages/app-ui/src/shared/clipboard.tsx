import { createContext, useContext } from "react";

export type CopyText = (text: string) => Promise<void>;

export const browserCopyText: CopyText = async (text) => {
  await navigator.clipboard.writeText(text);
};

// Web uses the browser API; Desktop injects its narrow, write-only Host method.
export const ClipboardContext = createContext<CopyText>(browserCopyText);
export function useCopyText(): CopyText { return useContext(ClipboardContext); }
