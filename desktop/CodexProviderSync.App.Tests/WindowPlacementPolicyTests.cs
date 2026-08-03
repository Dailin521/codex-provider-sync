using System.Drawing;
using System.Windows.Forms;
using CodexProviderSync.Core;

namespace CodexProviderSync.App.Tests;

public sealed class WindowPlacementPolicyTests
{
    private static readonly Rectangle PrimaryWorkingArea = new(0, 0, 1920, 1040);
    private static readonly Size DefaultSize = new(1280, 820);
    private static readonly Size MinimumSavedSize = new(800, 600);

    [Fact]
    public void Restore_RecentersBoundsWhenOnlyATinyHorizontalSliverIsVisible()
    {
        WindowBoundsState saved = Bounds(1910, 100, 1280, 820);

        WindowPlacement placement = Restore(saved, [PrimaryWorkingArea]);

        Assert.Equal(new Rectangle(320, 110, 1280, 820), placement.Bounds);
        Assert.False(placement.Maximized);
    }

    [Fact]
    public void Restore_PreservesBoundsWhenAUsablePartOfTheTitleBarIsVisible()
    {
        WindowBoundsState saved = Bounds(1800, 100, 1280, 820);

        WindowPlacement placement = Restore(saved, [PrimaryWorkingArea]);

        Assert.Equal(new Rectangle(1800, 100, 1280, 820), placement.Bounds);
        Assert.False(placement.Maximized);
    }

    [Fact]
    public void Restore_RecentersBoundsWhenContentIsVisibleButTheTitleBarIsOffscreen()
    {
        WindowBoundsState saved = Bounds(100, -100, 1200, 800);

        WindowPlacement placement = Restore(saved, [PrimaryWorkingArea]);

        Assert.Equal(new Rectangle(360, 120, 1200, 800), placement.Bounds);
        Assert.False(placement.Maximized);
    }

    [Fact]
    public void Restore_PreservesBoundsOnANegativeCoordinateDisplay()
    {
        Rectangle secondaryWorkingArea = new(-1600, 0, 1600, 860);
        WindowBoundsState saved = Bounds(-1500, 40, 1200, 760);

        WindowPlacement placement = Restore(saved, [PrimaryWorkingArea, secondaryWorkingArea]);

        Assert.Equal(new Rectangle(-1500, 40, 1200, 760), placement.Bounds);
    }

    [Fact]
    public void Restore_RecentersBoundsWhenTheirSecondaryDisplayWasRemoved()
    {
        WindowBoundsState saved = Bounds(2100, 100, 1200, 800);

        WindowPlacement placement = Restore(saved, [PrimaryWorkingArea]);

        Assert.Equal(new Rectangle(360, 120, 1200, 800), placement.Bounds);
        Assert.False(placement.Maximized);
    }

    [Fact]
    public void Restore_RecentersNormalBoundsBeforeRestoringMaximizedState()
    {
        WindowBoundsState saved = Bounds(2100, 100, 1200, 800, maximized: true);

        WindowPlacement placement = Restore(saved, [PrimaryWorkingArea]);

        Assert.Equal(new Rectangle(360, 120, 1200, 800), placement.Bounds);
        Assert.True(placement.Maximized);
    }

    [Fact]
    public void Restore_FitsRecoveredBoundsToASmallerCurrentDisplay()
    {
        Rectangle smallerWorkingArea = new(0, 0, 1440, 800);
        WindowBoundsState saved = Bounds(2000, 0, 1600, 900);

        WindowPlacement placement = Restore(saved, [smallerWorkingArea], smallerWorkingArea);

        Assert.Equal(smallerWorkingArea, placement.Bounds);
    }

    [Fact]
    public void Restore_UsesCenteredDefaultForMissingOrInvalidSettings()
    {
        WindowPlacement missing = Restore(null, [PrimaryWorkingArea]);
        WindowPlacement invalid = Restore(Bounds(100, 100, 799, 599), [PrimaryWorkingArea]);

        Rectangle expected = new(320, 110, 1280, 820);
        Assert.Equal(expected, missing.Bounds);
        Assert.Equal(expected, invalid.Bounds);
        Assert.False(missing.Maximized);
        Assert.False(invalid.Maximized);
    }

    [Theory]
    [InlineData(FormWindowState.Minimized, false)]
    [InlineData(FormWindowState.Maximized, true)]
    public void Capture_UsesRestoreBoundsForNonNormalStates(
        FormWindowState state,
        bool expectedMaximized)
    {
        Rectangle current = new(0, 0, 1920, 1040);
        Rectangle restore = new(200, 120, 1280, 820);

        WindowBoundsState captured = WindowPlacementPolicy.Capture(current, restore, state);

        Assert.Equal(restore.X, captured.X);
        Assert.Equal(restore.Y, captured.Y);
        Assert.Equal(restore.Width, captured.Width);
        Assert.Equal(restore.Height, captured.Height);
        Assert.Equal(expectedMaximized, captured.Maximized);
    }

    [Fact]
    public void Capture_UsesCurrentBoundsForNormalState()
    {
        Rectangle current = new(300, 180, 1180, 760);

        WindowBoundsState captured = WindowPlacementPolicy.Capture(
            current,
            new Rectangle(10, 10, 800, 600),
            FormWindowState.Normal);

        Assert.Equal(current.X, captured.X);
        Assert.Equal(current.Y, captured.Y);
        Assert.Equal(current.Width, captured.Width);
        Assert.Equal(current.Height, captured.Height);
        Assert.False(captured.Maximized);
    }

    private static WindowPlacement Restore(
        WindowBoundsState? saved,
        IReadOnlyList<Rectangle> workingAreas,
        Rectangle? fallbackWorkingArea = null)
    {
        return WindowPlacementPolicy.Restore(
            saved,
            workingAreas,
            fallbackWorkingArea ?? PrimaryWorkingArea,
            DefaultSize,
            MinimumSavedSize);
    }

    private static WindowBoundsState Bounds(
        int x,
        int y,
        int width,
        int height,
        bool maximized = false)
    {
        return new WindowBoundsState
        {
            X = x,
            Y = y,
            Width = width,
            Height = height,
            Maximized = maximized
        };
    }
}
