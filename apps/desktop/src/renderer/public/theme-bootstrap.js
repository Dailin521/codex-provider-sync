(() => {
  try {
    const theme = globalThis.localStorage.getItem("cps.desktop.theme");
    if (theme === "system" || theme === "light" || theme === "dark") {
      document.documentElement.dataset.theme = theme;
    }
  } catch {
    // Preferences are optional; the system theme remains the safe default.
  }
})();
