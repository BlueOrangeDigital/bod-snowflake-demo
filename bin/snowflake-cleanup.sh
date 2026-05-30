#!/bin/bash
# ============================================================================
# Snowflake Demo Cleanup
# ----------------------------------------------------------------------------
# Drops all pipeline-generated objects in AI_CORTEX_DEMO and truncates raw
# data tables. Leaves Terraform-managed schemas, warehouses, and table
# definitions intact.
#
# Usage:   bin/snowflake-cleanup.sh --yes [--connection <name>]
# Default connection: $SNOWSQL_CONNECTION or 'myaccount'
# ============================================================================
set -euo pipefail

CONFIRMED=0
CONNECTION="${SNOWSQL_CONNECTION:-demo}"

while [[ "$#" -gt 0 ]]; do
    case "$1" in
        --yes|-y)
            CONFIRMED=1
            shift
            ;;
        --connection|-c)
            CONNECTION="$2"
            shift 2
            ;;
        --help|-h)
            sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            echo "Usage: $0 --yes [--connection <name>]" >&2
            exit 2
            ;;
    esac
done

if [[ "$CONFIRMED" -ne 1 ]]; then
    cat >&2 <<EOF
⚠️  This will DROP all pipeline-generated objects in AI_CORTEX_DEMO,
   TRUNCATE all raw data tables (STOCK_PRICES, SEC_FILINGS, REAL_ESTATE),
   and DELETE demo-created SQL files from your Snowsight workspace.
   Terraform-managed schemas, warehouses, and table definitions are kept.

   Re-run with --yes to confirm:
     bin/snowflake-cleanup.sh --yes
EOF
    exit 1
fi

if ! command -v snowsql >/dev/null 2>&1; then
    echo "Error: snowsql not found on PATH. Install SnowSQL or activate the env." >&2
    exit 127
fi

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SQL_FILE="${SCRIPT_DIR}/../sql/cleanup.sql"

if [[ ! -f "$SQL_FILE" ]]; then
    echo "Error: cleanup SQL not found at $SQL_FILE" >&2
    exit 1
fi

echo "🧹 Running Snowflake cleanup via snowsql (connection: $CONNECTION)…"
snowsql -c "$CONNECTION" -f "$SQL_FILE"
echo "✅ SQL cleanup complete."

# ── Workspace cleanup (Snowsight UI files) ──────────────────────────────────
WORKSPACE_SCRIPT="${SCRIPT_DIR}/../demo/cleanup_workspaces.js"
if [[ -f "$WORKSPACE_SCRIPT" ]] && command -v node >/dev/null 2>&1; then
    echo ""
    echo "🧹 Running Snowsight workspace cleanup (Playwright)…"
    if node "$WORKSPACE_SCRIPT" --yes; then
        echo "✅ Workspace cleanup complete."
    else
        echo "⚠️  Workspace cleanup script failed. You may need to delete files manually" >&2
        echo "    from the Snowsight UI under Projects → Workspaces." >&2
    fi
else
    echo ""
    echo "ℹ️  Skipping workspace cleanup (node or $WORKSPACE_SCRIPT not found)."
    echo "   Delete demo files manually from Snowsight → Projects → Workspaces."
fi
