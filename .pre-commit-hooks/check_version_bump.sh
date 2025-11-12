#!/usr/bin/env bash
set -euo pipefail

FILE="aodn_cloud_optimised/lib/DataQuery.py"

# Check if the file is staged for commit
if git diff --cached --name-only | grep -q "^${FILE}$"; then
  # Extract the staged version (new) and HEAD version (old)
  old_version=$(git show HEAD:"${FILE}" 2>/dev/null | grep '^__version__' | sed -E 's/__version__ = "(.*)"/\1/')
  new_version=$(git show :${FILE} | grep '^__version__' | sed -E 's/__version__ = "(.*)"/\1/')

  if [ -z "$new_version" ]; then
    echo "❌ Error: Could not find __version__ line in ${FILE}"
    exit 1
  fi

  if [ "$old_version" = "$new_version" ]; then
    echo "❌ Version bump required!"
    echo "   The __version__ in ${FILE} has not changed (still ${new_version})."
    echo "   Please increment it before committing."
    exit 1
  else
    echo "✅ Version changed from ${old_version:-<none>} → ${new_version}"
  fi
fi

exit 0
