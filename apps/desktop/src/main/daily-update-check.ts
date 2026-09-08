import fs from "node:fs/promises";
import path from "node:path";

/** Claim a local calendar day before checking, including failed network attempts.
 * Exclusive creation also protects multiple launches sharing the same userData.
 * Only dates are persisted; this never touches a Codex Home.
 */
export async function claimDailyUpdateCheck(userData: string, now = new Date()): Promise<boolean> {
  const day = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-${String(now.getDate()).padStart(2, "0")}`;
  const directory = path.join(userData, "update-check-days");
  try {
    await fs.mkdir(directory, { recursive: true });
    const claim = await fs.open(path.join(directory, `${day}.checked`), "wx");
    await claim.close();
    return true;
  } catch {
    // An existing claim or unavailable persistence must not create repeated checks.
    return false;
  }
}
