#!/usr/bin/env bash
set -euo pipefail

FILE="aodn_cloud_optimised/lib/DataQuery.py"

declare -A EXPECTED_VARS=(
  ["REGION"]='REGION: Final[str] = "ap-southeast-2"'
  ["ENDPOINT_URL"]='ENDPOINT_URL = "https://s3.ap-southeast-2.amazonaws.com"'
  ["BUCKET_OPTIMISED_DEFAULT"]='BUCKET_OPTIMISED_DEFAULT = "aodn-cloud-optimised"'
  ["ROOT_PREFIX_CLOUD_OPTIMISED_PATH"]='ROOT_PREFIX_CLOUD_OPTIMISED_PATH = ""'
)

# Run only if file is staged
if git diff --cached --name-only | grep -q "^${FILE}$"; then
  echo "🔍 Checking protected environment constants in ${FILE}..."
  staged_content=$(git show :${FILE})

  failed=0
  for key in "${!EXPECTED_VARS[@]}"; do
    expected="${EXPECTED_VARS[$key]}"
    actual_line=$(echo "$staged_content" | grep -E "^${key}")
    if [[ "$actual_line" != "$expected" ]]; then
      echo "❌ ${key} does not match expected value!"
      echo "   Expected: ${expected}"
      echo "   Found:    ${actual_line:-<missing>}"
      failed=1
    fi
  done

  if [[ $failed -eq 1 ]]; then
    echo
    echo "🚫 Commit blocked: one or more protected environment constants were modified."
    echo "   To intentionally change these, update this pre-commit script as well."
    exit 1
  else
    echo "✅ All protected environment constants match expected values."
  fi
fi

exit 0
