using Avalonia;
using Avalonia.Controls;
using Avalonia.Layout;
using Avalonia.Media;

namespace CodexProviderSync.Mac;

internal sealed class ConfirmationDialog : Window
{
    public ConfirmationDialog(string title, string message, string primaryText, string? secondaryText)
    {
        Title = title;
        Width = 460;
        SizeToContent = SizeToContent.Height;
        WindowStartupLocation = WindowStartupLocation.CenterOwner;
        CanResize = false;
        Background = Brushes.White;

        StackPanel root = new()
        {
            Margin = new Thickness(20),
            Spacing = 18
        };

        root.Children.Add(new TextBlock
        {
            Text = message,
            TextWrapping = TextWrapping.Wrap,
            LineHeight = 20,
            Foreground = Brush.Parse("#18181b")
        });

        StackPanel buttons = new()
        {
            Orientation = Orientation.Horizontal,
            HorizontalAlignment = HorizontalAlignment.Right,
            Spacing = 10
        };

        if (!string.IsNullOrWhiteSpace(secondaryText))
        {
            Button secondary = new()
            {
                Content = secondaryText,
                MinWidth = 88
            };
            secondary.Click += (_, _) => Close(false);
            buttons.Children.Add(secondary);
        }

        Button primary = new()
        {
            Content = primaryText,
            MinWidth = 88
        };
        primary.Click += (_, _) => Close(true);
        buttons.Children.Add(primary);
        root.Children.Add(buttons);

        Content = root;
    }
}
