using CodexProviderSync.GuiE2E;

namespace CodexProviderSync.GuiE2E.Tests;

public sealed class EvidenceRedactorTests
{
    [Fact]
    public void Redact_RemovesDescriptorTokenFromMachineReadableEvidence()
    {
        var token = string.Concat(Enumerable.Repeat("0123456789abcdef", 4));
        var evidence = $$"""
            {
              "schemaVersion": 1,
              "descriptor": { "token": "{{token}}" },
              "diagnostic": "named-pipe authentication used {{token}}",
              "overallPassed": false
            }
            """;

        var redacted = EvidenceRedactor.Redact(evidence, [token]);

        Assert.DoesNotContain(token, redacted, StringComparison.Ordinal);
        Assert.Contains("schemaVersion", redacted, StringComparison.Ordinal);
        Assert.Contains("overallPassed", redacted, StringComparison.Ordinal);
    }

    [Fact]
    public void Redact_RemovesEverySecretAndIgnoresEmptySecretValues()
    {
        const string firstSecret = "first-private-value";
        const string secondSecret = "second-private-value";
        const string evidence = "first-private-value|safe-marker|second-private-value";

        var redacted = EvidenceRedactor.Redact(evidence, [firstSecret, "", secondSecret]);

        Assert.DoesNotContain(firstSecret, redacted, StringComparison.Ordinal);
        Assert.DoesNotContain(secondSecret, redacted, StringComparison.Ordinal);
        Assert.Contains("safe-marker", redacted, StringComparison.Ordinal);
    }
}
