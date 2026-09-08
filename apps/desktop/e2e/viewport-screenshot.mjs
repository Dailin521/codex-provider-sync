// Hidden Electron surfaces can stall a capture after viewport emulation.
// Capture only the current viewport (no element-driven resize), and retry only
// a capture timeout once. Assertions and repeated capture failures still fail.
export async function captureViewport(page, options) {
  try {
    return await page.screenshot({ ...options, timeout: 10_000 });
  } catch (error) {
    if (error?.name !== "TimeoutError" || !/page\.screenshot: Timeout/.test(error.message)) throw error;
    process.stderr.write("Hidden viewport capture timed out; retrying once without resizing or showing the window.\n");
    return page.screenshot({ ...options, timeout: 20_000 });
  }
}
