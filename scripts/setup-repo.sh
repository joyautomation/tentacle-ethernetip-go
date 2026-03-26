#!/usr/bin/env bash
set -euo pipefail

# Setup script for GitHub repo configuration:
# - Ensures repo is public
# - Configures branch protection on main
# - Requires CI status checks to pass
# - Requires PRs (0 approvals, self-merge OK)
#
# Prerequisites: gh CLI authenticated
#
# Usage: ./scripts/setup-repo.sh [owner/repo]
#   If no argument, detects from git remote.

REPO="${1:-}"
if [[ -z "$REPO" ]]; then
  REPO=$(gh repo view --json nameWithOwner -q .nameWithOwner 2>/dev/null) || {
    echo "Error: could not detect repo. Pass owner/repo as argument." >&2
    exit 1
  }
fi

echo "Configuring repo: $REPO"

# Ensure public (required for free-tier branch protection)
VISIBILITY=$(gh repo view "$REPO" --json visibility -q .visibility)
if [[ "$VISIBILITY" != "PUBLIC" ]]; then
  echo "Making repo public..."
  gh repo edit "$REPO" --visibility public --accept-visibility-change-consequences
else
  echo "Repo is already public."
fi

# CI job names — these must match the job names in ci.yml
CI_CHECKS=(
  "test (linux-amd64, ubuntu-latest)"
  "test (linux-arm64, ubuntu-24.04-arm)"
)

# Build the status checks JSON array
CHECKS_JSON=$(printf '%s\n' "${CI_CHECKS[@]}" | jq -R '.' | jq -s 'map({context: ., app_id: null})')
CONTEXTS_JSON=$(printf '%s\n' "${CI_CHECKS[@]}" | jq -R '.' | jq -s '.')

echo "Setting branch protection on main..."
gh api "repos/$REPO/branches/main/protection" \
  --method PUT \
  --input - <<EOF
{
  "required_status_checks": {
    "strict": false,
    "contexts": $CONTEXTS_JSON
  },
  "enforce_admins": false,
  "required_pull_request_reviews": {
    "required_approving_review_count": 0
  },
  "restrictions": null
}
EOF

echo ""
echo "Done. Branch protection configured:"
echo "  - PRs required to merge to main"
echo "  - CI checks must pass: ${CI_CHECKS[*]}"
echo "  - 0 approvals required (self-merge OK)"
echo "  - Force push blocked"
