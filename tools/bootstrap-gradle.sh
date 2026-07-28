#!/usr/bin/env bash
# Creates the Gradle wrapper, once.
#
# There is a chicken-and-egg problem before the wrapper (gradlew + gradle-wrapper.jar)
# exists in the repository: generating the wrapper needs Gradle. This script downloads a
# Gradle distribution into a temporary directory, runs `gradle wrapper`, and removes it
# again. From then on everyone just uses ./gradlew — no Gradle installation required.
#
# Usage:  bash tools/bootstrap-gradle.sh

set -euo pipefail

GRADLE_VERSION="9.6.1"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

cd "$REPO_ROOT"

if [ -x ./gradlew ]; then
  echo "Wrapper already present: $(./gradlew --version | grep -i '^Gradle' || true)"
  echo "To regenerate, first delete gradlew, gradlew.bat and gradle/wrapper/."
  exit 0
fi

if ! command -v java >/dev/null 2>&1; then
  echo "ERROR: java not found. Install JDK 25 first:" >&2
  echo "  sudo apt install -y openjdk-25-jdk" >&2
  exit 1
fi

if ! command -v unzip >/dev/null 2>&1; then
  echo "ERROR: unzip not found. Install it first:" >&2
  echo "  sudo apt install -y unzip" >&2
  exit 1
fi

echo "Java: $(java -version 2>&1 | head -1)"
echo "Downloading Gradle ${GRADLE_VERSION}..."

ZIP="$TMP_DIR/gradle.zip"
curl -fsSL -o "$ZIP" \
  "https://services.gradle.org/distributions/gradle-${GRADLE_VERSION}-bin.zip"

unzip -q "$ZIP" -d "$TMP_DIR"
GRADLE_BIN="$TMP_DIR/gradle-${GRADLE_VERSION}/bin/gradle"

echo "Generating wrapper..."
"$GRADLE_BIN" wrapper --gradle-version "$GRADLE_VERSION" --no-daemon

chmod +x ./gradlew

echo
echo "Done. From now on use:"
echo "  ./gradlew check"
