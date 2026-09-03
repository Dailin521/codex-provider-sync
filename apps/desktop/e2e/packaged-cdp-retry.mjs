export function isPackagedCdpConnectTimeout(error) {
  return error instanceof Error
    && error.name === "TimeoutError"
    && error.message.includes("connectOverCDP");
}

export function shouldRetryPackagedCdpActivation({
  platform,
  attempt,
  endpointReady,
  browserConnected,
  cleanupCompleted,
  error
}) {
  return platform === "win32"
    && attempt === 1
    && endpointReady === true
    && browserConnected === false
    && cleanupCompleted === true
    && isPackagedCdpConnectTimeout(error);
}
