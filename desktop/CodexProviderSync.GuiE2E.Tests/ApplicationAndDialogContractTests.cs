using System.Text.Json.Nodes;

namespace CodexProviderSync.GuiE2E.Tests;

public sealed class ApplicationLinkVerifierTests
{
    [Fact]
    public void Verify_AcceptsExactPrimaryAndNestedSuccessfulOperation()
    {
        VerifiedApplicationLink verified = ApplicationLinkVerifier.Verify(
            Result("operation-1", "Sync", "Succeeded"),
            "Sync",
            "Succeeded");

        Assert.Equal("operation-1", verified.OperationId);
        Assert.Equal("Sync", verified.Operation);
        Assert.Equal("Succeeded", verified.Lifecycle);
        Assert.Empty(verified.ErrorCodes);
    }

    [Fact]
    public void Verify_AcceptsExpectedFailedStatusOperationWithExactErrorCode()
    {
        VerifiedApplicationLink verified = ApplicationLinkVerifier.Verify(
            Result("operation-failure-1", "Status", "Failed", "operation_failed"),
            "Status",
            "Failed",
            ["operation_failed"]);

        Assert.Equal("Failed", verified.Lifecycle);
        Assert.Equal(["operation_failed"], verified.ErrorCodes);
    }

    [Fact]
    public void Verify_RejectsWrongOperationLifecycleAndUnexpectedErrors()
    {
        Assert.Throws<InvalidDataException>(() =>
            ApplicationLinkVerifier.Verify(Result("operation-1", "Switch", "Succeeded"), "Sync", "Succeeded"));
        Assert.Throws<InvalidDataException>(() =>
            ApplicationLinkVerifier.Verify(Result("operation-1", "Sync", "Rejected"), "Sync", "Succeeded"));
        Assert.Throws<InvalidDataException>(() =>
            ApplicationLinkVerifier.Verify(Result("operation-1", "Sync", "Succeeded", "plan_stale"), "Sync", "Succeeded"));
    }

    [Fact]
    public void Verify_RejectsMissingMismatchedOrDuplicateNestedAssociation()
    {
        JsonObject mismatched = Result("operation-1", "Restore", "Succeeded");
        mismatched["applicationOperations"]![0]!["operation"] = "Sync";
        Assert.Throws<InvalidDataException>(() =>
            ApplicationLinkVerifier.Verify(mismatched, "Restore", "Succeeded"));

        JsonObject duplicate = Result("operation-1", "Prune", "Succeeded");
        duplicate["applicationOperations"]!.AsArray().Add(
            duplicate["applicationOperations"]![0]!.DeepClone());
        Assert.Throws<InvalidDataException>(() =>
            ApplicationLinkVerifier.Verify(duplicate, "Prune", "Succeeded"));

        JsonObject missing = Result("operation-1", "Status", "Succeeded");
        missing["applicationOperations"] = new JsonArray();
        Assert.Throws<InvalidDataException>(() =>
            ApplicationLinkVerifier.Verify(missing, "Status", "Succeeded"));
    }

    private static JsonObject Result(
        string operationId,
        string operation,
        string lifecycle,
        params string[] errors) => new()
    {
        ["applicationOperationLinked"] = true,
        ["applicationOperationId"] = operationId,
        ["applicationOperation"] = operation,
        ["applicationLifecycle"] = lifecycle,
        ["applicationOperations"] = new JsonArray
        {
            new JsonObject
            {
                ["operationId"] = operationId,
                ["operation"] = operation,
                ["lifecycle"] = lifecycle,
                ["errorCodes"] = new JsonArray(
                    errors.Select(error => (JsonNode?)JsonValue.Create(error)).ToArray())
            }
        }
    };
}

public sealed class DialogContractVerifierTests
{
    [Fact]
    public void Verify_AcceptsExpectedConfirmationAndNoUpdateDialogs()
    {
        DialogContractResult sync = DialogContractVerifier.Verify(
            "sync-apply",
            "#32770",
            "Codex Provider Sync",
            ["执行同步前，请先关闭 Codex。是否继续？", "确定", "取消"]);
        DialogContractResult noUpdate = DialogContractVerifier.Verify(
            "update-no-update",
            "#32770",
            "Codex Provider Sync",
            ["当前已是最新版本（v0.5.0）。", "确定"]);

        Assert.True(sync.Passed, sync.Error);
        Assert.True(noUpdate.Passed, noUpdate.Error);
    }

    [Fact]
    public void Verify_RejectsFailureMasqueradingAsNoUpdateAndMissingCancel()
    {
        DialogContractResult failure = DialogContractVerifier.Verify(
            "update-no-update",
            "#32770",
            "Codex Provider Sync",
            ["更新检查失败：network error", "确定"]);
        DialogContractResult missingCancel = DialogContractVerifier.Verify(
            "prune-apply",
            "#32770",
            "Codex Provider Sync",
            ["确认清理旧备份？将只保留最近 1 份。", "确定"]);

        Assert.False(failure.Passed);
        Assert.Contains("forbidden", failure.Error, StringComparison.OrdinalIgnoreCase);
        Assert.False(missingCancel.Passed);
        Assert.Contains("cancel", missingCancel.Error, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Verify_OperationFailureRequiresErrorTextAndAcknowledgementOnly()
    {
        DialogContractResult valid = DialogContractVerifier.Verify(
            "operation-failure",
            "#32770",
            "Codex Provider Sync",
            ["Invalid config.toml: unterminated string.", "确定"]);
        DialogContractResult confirmation = DialogContractVerifier.Verify(
            "operation-failure",
            "#32770",
            "Codex Provider Sync",
            ["是否继续？", "确定", "取消"]);

        Assert.True(valid.Passed, valid.Error);
        Assert.False(confirmation.Passed);
    }

    [Theory]
    [InlineData("NotADialog", "Codex Provider Sync")]
    [InlineData("#32770", "Unexpected title")]
    public void Verify_RejectsWrongNativeClassOrTitle(string className, string title)
    {
        DialogContractResult result = DialogContractVerifier.Verify(
            "validation",
            className,
            title,
            ["请输入要添加的 Provider ID。", "确定"]);

        Assert.False(result.Passed);
    }
}
