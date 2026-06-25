#!/usr/bin/env bash
set -euo pipefail
REMOTE="${1:-git@github.com:mplummeridge/ha-codexbar-fleet.git}"
BRANCH="${2:-main}"
if [ ! -d .git ]; then
  git init -b "$BRANCH"
fi
git add -A
git commit -m "feat: add CodexBar Fleet HACS integration" || true
git remote remove origin 2>/dev/null || true
git remote add origin "$REMOTE"
git push -u origin "$BRANCH" --force-with-lease
