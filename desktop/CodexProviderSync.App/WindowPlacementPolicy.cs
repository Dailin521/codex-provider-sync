using CodexProviderSync.Core;

namespace CodexProviderSync.App;

internal readonly record struct WindowPlacement(Rectangle Bounds, bool Maximized);

internal static class WindowPlacementPolicy
{
    private const int TitleBarProbeHeight = 32;
    private const int MinimumVisibleTitleBarWidth = 120;
    private const int MinimumVisibleTitleBarHeight = 24;

    internal static WindowBoundsState Capture(
        Rectangle currentBounds,
        Rectangle restoreBounds,
        FormWindowState windowState)
    {
        Rectangle bounds = windowState == FormWindowState.Normal ? currentBounds : restoreBounds;
        return new WindowBoundsState
        {
            X = bounds.X,
            Y = bounds.Y,
            Width = bounds.Width,
            Height = bounds.Height,
            Maximized = windowState == FormWindowState.Maximized
        };
    }

    internal static WindowPlacement Restore(
        WindowBoundsState? saved,
        IReadOnlyList<Rectangle> workingAreas,
        Rectangle fallbackWorkingArea,
        Size defaultSize,
        Size minimumSavedSize)
    {
        Rectangle safeWorkingArea = NormalizeWorkingArea(fallbackWorkingArea, defaultSize);
        bool hasValidSavedSize = saved is not null
            && saved.Width >= minimumSavedSize.Width
            && saved.Height >= minimumSavedSize.Height;

        if (!hasValidSavedSize)
        {
            return new WindowPlacement(CenterAndFit(defaultSize, safeWorkingArea), Maximized: false);
        }

        Rectangle savedBounds = new(saved!.X, saved.Y, saved.Width, saved.Height);
        if (workingAreas.Any(area => HasUsableVisibleTitleBar(savedBounds, area)))
        {
            return new WindowPlacement(savedBounds, saved.Maximized);
        }

        // A previously attached display may no longer exist. Keep the user's
        // normal window size where possible, but move it onto the current
        // primary display before applying a saved maximized state.
        Rectangle recoveredBounds = CenterAndFit(savedBounds.Size, safeWorkingArea);
        return new WindowPlacement(recoveredBounds, saved.Maximized);
    }

    private static bool HasUsableVisibleTitleBar(Rectangle windowBounds, Rectangle workingArea)
    {
        if (workingArea.Width <= 0 || workingArea.Height <= 0)
        {
            return false;
        }

        // A few visible content pixels are not enough to recover a window: the
        // user needs a usable section of the title bar to drag it back. Use
        // Int64 arithmetic so malformed settings near Int32 limits cannot
        // overflow while calculating either rectangle.
        long windowRight = (long)windowBounds.X + windowBounds.Width;
        long areaRight = (long)workingArea.X + workingArea.Width;
        long areaBottom = (long)workingArea.Y + workingArea.Height;
        long titleBarBottom = Math.Min(
            (long)windowBounds.Y + windowBounds.Height,
            (long)windowBounds.Y + TitleBarProbeHeight);

        long visibleWidth = Math.Min(windowRight, areaRight)
            - Math.Max((long)windowBounds.X, workingArea.X);
        long visibleHeight = Math.Min(titleBarBottom, areaBottom)
            - Math.Max((long)windowBounds.Y, workingArea.Y);

        return visibleWidth >= MinimumVisibleTitleBarWidth
            && visibleHeight >= MinimumVisibleTitleBarHeight;
    }

    private static Rectangle CenterAndFit(Size requestedSize, Rectangle workingArea)
    {
        int width = Math.Min(Math.Max(1, requestedSize.Width), workingArea.Width);
        int height = Math.Min(Math.Max(1, requestedSize.Height), workingArea.Height);
        int x = workingArea.X + ((workingArea.Width - width) / 2);
        int y = workingArea.Y + ((workingArea.Height - height) / 2);
        return new Rectangle(x, y, width, height);
    }

    private static Rectangle NormalizeWorkingArea(Rectangle workingArea, Size defaultSize)
    {
        if (workingArea.Width > 0 && workingArea.Height > 0)
        {
            return workingArea;
        }

        return new Rectangle(
            0,
            0,
            Math.Max(1, defaultSize.Width),
            Math.Max(1, defaultSize.Height));
    }
}
