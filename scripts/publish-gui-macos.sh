#!/usr/bin/env bash
set -euo pipefail

configuration="Release"
runtime="osx-arm64"
output="artifacts/osx-arm64"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --configuration)
      configuration="${2:?Missing value for --configuration}"
      shift 2
      ;;
    --runtime)
      runtime="${2:?Missing value for --runtime}"
      shift 2
      ;;
    --output)
      output="${2:?Missing value for --output}"
      shift 2
      ;;
    -h|--help)
      cat <<'EOF'
Usage: scripts/publish-gui-macos.sh [--configuration Release] [--runtime osx-arm64] [--output artifacts/osx-arm64]

Publishes the Avalonia macOS GUI as a self-contained app bundle.
Set DOTNET=/path/to/dotnet to use a specific SDK.
EOF
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
project="$repo_root/desktop/CodexProviderSync.Mac/CodexProviderSync.Mac.csproj"
output_dir="$repo_root/$output"
publish_dir="$output_dir/publish"
app_dir="$output_dir/CodexProviderSync.app"
contents_dir="$app_dir/Contents"
macos_dir="$contents_dir/MacOS"
resources_dir="$contents_dir/Resources"
dotnet="${DOTNET:-dotnet}"

if [[ "$runtime" != osx-* ]]; then
  echo "Runtime must be a macOS RID such as osx-arm64 or osx-x64." >&2
  exit 1
fi

if [[ -e "$output_dir" ]]; then
  rm -rf "$output_dir"
fi

mkdir -p "$publish_dir" "$macos_dir" "$resources_dir"

"$dotnet" publish "$project" \
  --runtime "$runtime" \
  -c "$configuration" \
  --self-contained true \
  -o "$publish_dir" \
  /p:PublishSingleFile=true \
  /p:IncludeNativeLibrariesForSelfExtract=true \
  /p:EnableCompressionInSingleFile=true \
  /p:DebugType=None \
  /p:DebugSymbols=false \
  /p:PublishTrimmed=false

cp -R "$publish_dir"/. "$macos_dir"/
chmod +x "$macos_dir/CodexProviderSync"

cat > "$contents_dir/Info.plist" <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleDevelopmentRegion</key>
  <string>en</string>
  <key>CFBundleDisplayName</key>
  <string>Codex Provider Sync</string>
  <key>CFBundleExecutable</key>
  <string>CodexProviderSync</string>
  <key>CFBundleIdentifier</key>
  <string>com.dailin521.codexprovidersync</string>
  <key>CFBundleInfoDictionaryVersion</key>
  <string>6.0</string>
  <key>CFBundleName</key>
  <string>Codex Provider Sync</string>
  <key>CFBundlePackageType</key>
  <string>APPL</string>
  <key>CFBundleShortVersionString</key>
  <string>0.3.1</string>
  <key>CFBundleVersion</key>
  <string>0.3.1</string>
  <key>LSMinimumSystemVersion</key>
  <string>12.0</string>
  <key>NSHighResolutionCapable</key>
  <true/>
</dict>
</plist>
EOF

rm -rf "$publish_dir"

if command -v codesign >/dev/null 2>&1; then
  codesign --force --deep --sign - "$app_dir" >/dev/null
fi

echo "macOS GUI app published to $app_dir"
