using System.Globalization;
using CodexProviderSync.Application;
using CodexProviderSync.Core;

namespace CodexProviderSync.Automation;

public static class AutomationCommandLine
{
    private const int MaximumTimeoutMilliseconds = 30 * 60 * 1000;

    private static readonly HashSet<string> ValueOptions = new(StringComparer.Ordinal)
    {
        "--codex-home",
        "--sqlite-home",
        "--ledger-root",
        "--timeout-ms",
        "--operation",
        "--provider",
        "--keep",
        "--model-mode",
        "--model",
        "--backup",
        "--plan",
        "--plan-digest"
    };

    private static readonly HashSet<string> FlagOptions = new(StringComparer.Ordinal)
    {
        "--apply",
        "--no-config",
        "--no-database",
        "--no-sessions",
        "--allow-sqlite-home-relocation"
    };

    public static AutomationParseResult Parse(IReadOnlyList<string> args)
    {
        if (args.Count == 0)
        {
            return Failure(string.Empty, "command_required", "A command is required.");
        }

        string commandName = args[0];
        if (!TryParseCommand(commandName, out AutomationCommand command))
        {
            return Failure(commandName, "unknown_command", $"Unknown command: {commandName}");
        }

        Dictionary<string, string> values = new(StringComparer.Ordinal);
        HashSet<string> flags = new(StringComparer.Ordinal);
        for (int index = 1; index < args.Count; index++)
        {
            string option = args[index];
            if (ValueOptions.Contains(option))
            {
                if (values.ContainsKey(option) || flags.Contains(option))
                {
                    return Failure(commandName, "duplicate_option", $"Option {option} may only be specified once.");
                }
                if (++index >= args.Count || args[index].StartsWith("--", StringComparison.Ordinal))
                {
                    return Failure(commandName, "option_value_required", $"Option {option} requires a value.");
                }
                values.Add(option, args[index]);
                continue;
            }
            if (FlagOptions.Contains(option))
            {
                if (!flags.Add(option) || values.ContainsKey(option))
                {
                    return Failure(commandName, "duplicate_option", $"Option {option} may only be specified once.");
                }
                continue;
            }

            return Failure(commandName, "unknown_option", $"Unknown option: {option}");
        }

        try
        {
            ValidateAllowedOptions(command, values.Keys.Concat(flags));
            TimeSpan? timeout = ParseTimeout(values.GetValueOrDefault("--timeout-ms"));
            if (command == AutomationCommand.Describe)
            {
                return Success(new AutomationInvocation(
                    command,
                    commandName,
                    null,
                    null,
                    false,
                    null,
                    null,
                    null,
                    timeout));
            }

            string codexHome = NormalizeDirectoryPath(
                Required(values, "--codex-home"),
                "--codex-home",
                mustExist: true);
            string? sqliteHome = OptionalDirectoryPath(
                values.GetValueOrDefault("--sqlite-home"),
                "--sqlite-home",
                mustExist: true);
            if (command == AutomationCommand.Status)
            {
                return Success(new AutomationInvocation(
                    command,
                    commandName,
                    new ApplicationStatusRequest(codexHome, sqliteHome),
                    null,
                    false,
                    null,
                    null,
                    null,
                    timeout));
            }

            string? ledgerRoot = OptionalDirectoryPath(values.GetValueOrDefault("--ledger-root"), "--ledger-root")
                ?? Path.Combine(codexHome, "tmp", "provider-sync-automation-ledger");
            AutomationCommand intentCommand = command == AutomationCommand.Plan
                ? ParseWriteCommand(Required(values, "--operation"))
                : command;
            if (command == AutomationCommand.Plan)
            {
                ValidatePlanOperationOptions(intentCommand, values.Keys.Concat(flags));
            }
            ApplicationWriteIntent intent = CreateIntent(intentCommand, codexHome, sqliteHome, values, flags);
            bool apply = flags.Contains("--apply");
            string? planPath = OptionalFilePath(values.GetValueOrDefault("--plan"), "--plan", mustExist: true);
            string? digest = NormalizeDigest(values.GetValueOrDefault("--plan-digest"));

            if (command == AutomationCommand.Plan && apply)
            {
                throw new AutomationUsageException("apply_not_allowed", "The plan command never applies changes.");
            }
            if (command == AutomationCommand.Plan && (planPath is not null || digest is not null))
            {
                throw new AutomationUsageException("plan_input_not_allowed", "The plan command does not accept an existing plan.");
            }
            if (!apply && (planPath is not null || digest is not null))
            {
                throw new AutomationUsageException(
                    "apply_required",
                    "--plan and --plan-digest are only accepted together with explicit --apply.");
            }
            if (apply && (planPath is null || digest is null))
            {
                throw new AutomationUsageException(
                    "plan_required",
                    "Explicit --apply requires both --plan and --plan-digest.");
            }

            return Success(new AutomationInvocation(
                command,
                commandName,
                null,
                intent,
                apply,
                planPath,
                digest,
                ledgerRoot,
                timeout));
        }
        catch (AutomationUsageException error)
        {
            return Failure(commandName, error.Code, error.Message);
        }
        catch (Exception error) when (error is ArgumentException or NotSupportedException or PathTooLongException)
        {
            return Failure(commandName, "invalid_input", error.Message);
        }
    }

    private static ApplicationWriteIntent CreateIntent(
        AutomationCommand command,
        string codexHome,
        string? sqliteHome,
        IReadOnlyDictionary<string, string> values,
        IReadOnlySet<string> flags)
    {
        int keep = ParsePositiveInteger(values.GetValueOrDefault("--keep") ?? AppConstants.DefaultBackupRetentionCount.ToString(CultureInfo.InvariantCulture), "--keep");
        return command switch
        {
            AutomationCommand.Sync => new SyncIntent(
                codexHome,
                sqliteHome,
                NormalizeRequiredText(Required(values, "--provider"), "--provider"),
                keep),
            AutomationCommand.Switch => new SwitchIntent(
                codexHome,
                sqliteHome,
                NormalizeRequiredText(Required(values, "--provider"), "--provider"),
                ParseModelSelection(values.GetValueOrDefault("--model-mode"), values.GetValueOrDefault("--model")),
                keep),
            AutomationCommand.Restore => new RestoreIntent(
                codexHome,
                sqliteHome,
                NormalizeDirectoryPath(Required(values, "--backup"), "--backup", mustExist: true),
                RestoreConfig: !flags.Contains("--no-config"),
                RestoreDatabase: !flags.Contains("--no-database"),
                RestoreSessions: !flags.Contains("--no-sessions"),
                AllowSqliteHomeRelocation: flags.Contains("--allow-sqlite-home-relocation")),
            AutomationCommand.Prune => new PruneIntent(codexHome, sqliteHome, keep),
            _ => throw new AutomationUsageException("invalid_operation", "A write operation is required.")
        };
    }

    private static SwitchModelSelection ParseModelSelection(string? mode, string? model)
    {
        string normalizedMode = string.IsNullOrWhiteSpace(mode) ? "follow-provider" : mode.Trim();
        return normalizedMode switch
        {
            "follow-provider" when model is null => new FollowProviderModelSelection(),
            "keep-root" when model is null => new KeepRootModelSelection(),
            "custom" => new CustomModelSelection(NormalizeRequiredText(model, "--model")),
            "follow-provider" or "keep-root" => throw new AutomationUsageException(
                "model_not_allowed",
                "--model is only valid with --model-mode custom."),
            _ => throw new AutomationUsageException(
                "model_mode_invalid",
                "--model-mode must be follow-provider, keep-root, or custom.")
        };
    }

    private static void ValidateAllowedOptions(AutomationCommand command, IEnumerable<string> options)
    {
        HashSet<string> allowed = command switch
        {
            AutomationCommand.Describe => ["--timeout-ms"],
            AutomationCommand.Status => ["--codex-home", "--sqlite-home", "--timeout-ms"],
            AutomationCommand.Plan =>
            [
                "--codex-home", "--sqlite-home", "--ledger-root", "--timeout-ms", "--operation",
                "--provider", "--keep", "--model-mode", "--model", "--backup", "--no-config",
                "--no-database", "--no-sessions", "--allow-sqlite-home-relocation"
            ],
            AutomationCommand.Sync => CommonWriteOptions("--provider", "--keep"),
            AutomationCommand.Switch => CommonWriteOptions("--provider", "--keep", "--model-mode", "--model"),
            AutomationCommand.Restore => CommonWriteOptions(
                "--backup", "--no-config", "--no-database", "--no-sessions", "--allow-sqlite-home-relocation"),
            AutomationCommand.Prune => CommonWriteOptions("--keep"),
            _ => throw new ArgumentOutOfRangeException(nameof(command))
        };

        string? invalid = options.FirstOrDefault(option => !allowed.Contains(option));
        if (invalid is not null)
        {
            throw new AutomationUsageException(
                "option_not_allowed",
                $"Option {invalid} is not valid for {ToCommandName(command)}.");
        }
    }

    private static void ValidatePlanOperationOptions(
        AutomationCommand operation,
        IEnumerable<string> options)
    {
        HashSet<string> allowed = new(StringComparer.Ordinal)
        {
            "--codex-home", "--sqlite-home", "--ledger-root", "--timeout-ms", "--operation"
        };
        allowed.UnionWith(operation switch
        {
            AutomationCommand.Sync => ["--provider", "--keep"],
            AutomationCommand.Switch => ["--provider", "--keep", "--model-mode", "--model"],
            AutomationCommand.Restore =>
            [
                "--backup", "--no-config", "--no-database", "--no-sessions",
                "--allow-sqlite-home-relocation"
            ],
            AutomationCommand.Prune => ["--keep"],
            _ => throw new ArgumentOutOfRangeException(nameof(operation))
        });
        string? invalid = options.FirstOrDefault(option => !allowed.Contains(option));
        if (invalid is not null)
        {
            throw new AutomationUsageException(
                "option_not_allowed",
                $"Option {invalid} is not valid for plan --operation {ToCommandName(operation)}.");
        }
    }

    private static HashSet<string> CommonWriteOptions(params string[] commandSpecific)
    {
        HashSet<string> result = new(StringComparer.Ordinal)
        {
            "--codex-home", "--sqlite-home", "--ledger-root", "--timeout-ms",
            "--apply", "--plan", "--plan-digest"
        };
        result.UnionWith(commandSpecific);
        return result;
    }

    private static string NormalizeAbsolutePath(string value, string option, bool mustExist)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new AutomationUsageException("path_required", $"{option} requires a non-empty path.");
        }
        if (!Path.IsPathFullyQualified(value))
        {
            throw new AutomationUsageException(
                "path_escape",
                $"{option} must be an absolute path; relative and escaping paths are not accepted.");
        }

        string fullPath = Path.GetFullPath(value);
        if (string.Equals(Path.GetFileName(fullPath), "auth.json", StringComparison.OrdinalIgnoreCase))
        {
            throw new AutomationUsageException("credential_path_forbidden", "Automation never reads auth.json.");
        }
        if (mustExist && !File.Exists(fullPath) && !Directory.Exists(fullPath))
        {
            throw new AutomationUsageException("path_not_found", $"{option} does not exist.");
        }
        EnsureNoReparseAncestors(fullPath, option);

        return fullPath;
    }

    private static string NormalizeDirectoryPath(string value, string option, bool mustExist)
    {
        string path = NormalizeAbsolutePath(value, option, mustExist);
        if (File.Exists(path))
        {
            throw new AutomationUsageException("path_type_invalid", $"{option} must be a directory path.");
        }
        return path;
    }

    private static string NormalizeFilePath(string value, string option, bool mustExist)
    {
        string path = NormalizeAbsolutePath(value, option, mustExist);
        if (Directory.Exists(path))
        {
            throw new AutomationUsageException("path_type_invalid", $"{option} must be a file path.");
        }
        return path;
    }

    private static string? OptionalDirectoryPath(string? value, string option, bool mustExist = false)
    {
        return value is null ? null : NormalizeDirectoryPath(value, option, mustExist);
    }

    private static string? OptionalFilePath(string? value, string option, bool mustExist = false)
    {
        return value is null ? null : NormalizeFilePath(value, option, mustExist);
    }

    private static void EnsureNoReparseAncestors(string fullPath, string option)
    {
        string? current = fullPath;
        while (!string.IsNullOrEmpty(current))
        {
            if ((File.Exists(current) || Directory.Exists(current))
                && (File.GetAttributes(current) & FileAttributes.ReparsePoint) != 0)
            {
                throw new AutomationUsageException(
                    "path_escape",
                    $"{option} cannot traverse a reparse point or symbolic link.");
            }

            string? parent = Path.GetDirectoryName(current);
            if (string.Equals(parent, current, StringComparison.Ordinal))
            {
                break;
            }
            current = parent;
        }
    }

    private static string? NormalizeDigest(string? digest)
    {
        if (digest is null)
        {
            return null;
        }
        if (digest.Length != 64 || digest.Any(static character => character is not (>= '0' and <= '9') and not (>= 'a' and <= 'f')))
        {
            throw new AutomationUsageException(
                "plan_digest_invalid",
                "--plan-digest must be an exact lowercase 64-character SHA-256 hexadecimal value.");
        }
        return digest;
    }

    private static TimeSpan? ParseTimeout(string? value)
    {
        if (value is null)
        {
            return null;
        }
        int milliseconds = ParsePositiveInteger(value, "--timeout-ms");
        if (milliseconds > MaximumTimeoutMilliseconds)
        {
            throw new AutomationUsageException(
                "timeout_invalid",
                $"--timeout-ms cannot exceed {MaximumTimeoutMilliseconds}.");
        }
        return TimeSpan.FromMilliseconds(milliseconds);
    }

    private static int ParsePositiveInteger(string value, string option)
    {
        if (!int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out int parsed) || parsed < 1)
        {
            throw new AutomationUsageException("integer_invalid", $"{option} must be a positive integer.");
        }
        return parsed;
    }

    private static string Required(IReadOnlyDictionary<string, string> values, string option)
    {
        return values.TryGetValue(option, out string? value)
            ? value
            : throw new AutomationUsageException("option_required", $"{option} is required.");
    }

    private static string NormalizeRequiredText(string? value, string option)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new AutomationUsageException("option_value_required", $"{option} requires a non-empty value.");
        }
        return value.Trim();
    }

    private static AutomationCommand ParseWriteCommand(string value)
    {
        return value switch
        {
            "sync" => AutomationCommand.Sync,
            "switch" => AutomationCommand.Switch,
            "restore" => AutomationCommand.Restore,
            "prune" => AutomationCommand.Prune,
            _ => throw new AutomationUsageException(
                "operation_invalid",
                "--operation must be sync, switch, restore, or prune.")
        };
    }

    private static bool TryParseCommand(string value, out AutomationCommand command)
    {
        command = value switch
        {
            "describe" => AutomationCommand.Describe,
            "status" => AutomationCommand.Status,
            "plan" => AutomationCommand.Plan,
            "sync" => AutomationCommand.Sync,
            "switch" => AutomationCommand.Switch,
            "restore" => AutomationCommand.Restore,
            "prune" => AutomationCommand.Prune,
            _ => default
        };
        return value is "describe" or "status" or "plan" or "sync" or "switch" or "restore" or "prune";
    }

    private static string ToCommandName(AutomationCommand command)
    {
        return command.ToString().ToLowerInvariant();
    }

    private static AutomationParseResult Success(AutomationInvocation invocation) => new(invocation, null);

    private static AutomationParseResult Failure(string command, string code, string message) =>
        new(null, AutomationProtocolResponse.UsageFailure(command, code, message));

    private sealed class AutomationUsageException(string code, string message) : Exception(message)
    {
        public string Code { get; } = code;
    }
}
