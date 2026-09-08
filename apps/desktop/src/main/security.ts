import type {
  App,
  Protocol,
  Session,
  WebContents
} from "electron";

import {
  DESKTOP_APP_SCHEME
} from "../shared/constants.js";
import { createRendererAssetResponse } from "./security-policy.js";

export function registerDesktopScheme(protocol: Protocol): void {
  protocol.registerSchemesAsPrivileged([{
    scheme: DESKTOP_APP_SCHEME,
    privileges: {
      standard: true,
      secure: true,
      supportFetchAPI: true,
      bypassCSP: false,
      allowServiceWorkers: false
    }
  }]);
}

export async function registerDesktopProtocol(
  protocol: Protocol,
  rendererRoot: string
): Promise<void> {
  await protocol.handle(
    DESKTOP_APP_SCHEME,
    (request) => createRendererAssetResponse(rendererRoot, request)
  );
}

function lockWebContents(contents: WebContents): void {
  contents.on("will-navigate", (event) => event.preventDefault());
  contents.on("will-attach-webview", (event) => event.preventDefault());
  contents.setWindowOpenHandler(() => ({ action: "deny" }));
}

export function installDesktopSecurity(app: App, session: Session): () => void {
  session.setPermissionCheckHandler(() => false);
  session.setPermissionRequestHandler((_webContents, _permission, callback) => callback(false));
  const onCreated = (_event: Electron.Event, contents: WebContents) => lockWebContents(contents);
  app.on("web-contents-created", onCreated);
  return () => {
    app.removeListener("web-contents-created", onCreated);
    session.setPermissionCheckHandler(null);
    session.setPermissionRequestHandler(null);
  };
}
