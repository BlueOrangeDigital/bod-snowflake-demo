#!/bin/bash
# Upload the most recent demo recording to a GitHub Release.
#
# Usage:
#   ./upload-video-to-github-release.sh                # auto-bump minor from latest release
#   ./upload-video-to-github-release.sh 1.20           # use v1.20-demo
#   ./upload-video-to-github-release.sh v2.0-demo      # use exact tag

set -euo pipefail

# ── Determine the version tag ────────────────────────────────────────────────
if [[ $# -gt 0 ]]; then
    TAG="$1"
    # Normalize "1.20" → "v1.20-demo"
    if [[ "$TAG" =~ ^[0-9]+\.[0-9]+$ ]]; then
        TAG="v${TAG}-demo"
    fi
else
    LATEST=$(gh release list --limit 1 --json tagName -q '.[].tagName' 2>/dev/null || true)
    if [[ -z "$LATEST" ]]; then
        TAG="v1.0-demo"
        echo "No previous releases found — starting at ${TAG}"
    elif [[ "$LATEST" =~ ^v([0-9]+)\.([0-9]+)(-.+)?$ ]]; then
        MAJOR="${BASH_REMATCH[1]}"
        MINOR="${BASH_REMATCH[2]}"
        SUFFIX="${BASH_REMATCH[3]:-}"
        NEW_MINOR=$((MINOR + 1))
        TAG="v${MAJOR}.${NEW_MINOR}${SUFFIX}"
        echo "Latest release: ${LATEST} → bumping to: ${TAG}"
    else
        echo "Error: latest tag '${LATEST}' doesn't match vMAJOR.MINOR pattern." >&2
        echo "       Pass an explicit version: ./upload-video-to-github-release.sh 2.0" >&2
        exit 1
    fi
fi

# Pull the human-readable version number out for the release title
if [[ "$TAG" =~ ^v([0-9]+\.[0-9]+) ]]; then
    VERSION_NUM="${BASH_REMATCH[1]}"
else
    VERSION_NUM="${TAG}"
fi

# ── Find the recording to upload ─────────────────────────────────────────────
WEBM=$(ls recordings/*.webm | head -1)
if [[ -z "$WEBM" ]]; then
    echo "Error: no .webm files found in recordings/" >&2
    exit 1
fi

# ── Publish ─────────────────────────────────────────────────────────────────
gh release create "$TAG" \
    --title "Snowflake AI & Cortex Demo — v${VERSION_NUM}" \
    --notes "$(cat recordings/chapters.txt)" \
    "$WEBM" \
    recordings/chapters.txt
