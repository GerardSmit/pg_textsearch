#!/usr/bin/env bash
# scripts/check.sh — silent build + test, terse output for context economy.
#
# Output:
#   OK 65/65                       — all green
#   FAILED: name1, name2           — listed tests failed; first ~40 diff lines printed
#   BUILD FAILED                   — compile error; error lines printed
#   INSTALL FAILED                 — make install error; tail printed
#   POSTGRES FAILED                — restart error; status printed
#
# Exit code 0 on success, 1 on any failure.
#
# Tunables via env:
#   PGPORT  (default 5434)
#   PG_BIN  (default /usr/lib/postgresql/17/bin)
#   PG_SVC  (default postgresql@17-main)
#   REGRESS (default empty = full suite; otherwise pass-through to make test)

set -u

PGPORT="${PGPORT:-5434}"
PG_BIN="${PG_BIN:-/usr/lib/postgresql/17/bin}"
PG_SVC="${PG_SVC:-postgresql@17-main}"
REGRESS="${REGRESS:-}"

PROJECT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT"

#-- 1. Build ---------------------------------------------------------------
build_log="$(mktemp)"
trap 'rm -f "$build_log"' EXIT

if ! make >"$build_log" 2>&1; then
    echo "BUILD FAILED"
    grep -iE "error:" "$build_log" | head -20
    exit 1
fi
# Catch warnings-as-errors / -Werror that don't trip make's exit code in odd configs
if grep -qE "^[^/].*: error:" "$build_log"; then
    echo "BUILD FAILED"
    grep -iE "error:" "$build_log" | grep -v "^/usr/include" | head -20
    exit 1
fi

#-- 2. Install -------------------------------------------------------------
if ! make install >"$build_log" 2>&1; then
    echo "INSTALL FAILED"
    tail -5 "$build_log"
    exit 1
fi

#-- 3. Restart Postgres so the new .so is loaded ---------------------------
# Try systemctl first; fall back to pg_ctl for environments without systemd.
PG_CONF="/etc/postgresql/17/main/postgresql.conf"
PG_DATA="/var/lib/postgresql/17/main"
PG_LOG="$PG_DATA/logfile"

if systemctl restart "$PG_SVC" 2>/dev/null; then
    : # systemd restart succeeded
else
    # Fallback: pg_ctl restart
    sudo -u postgres "$PG_BIN/pg_ctl" stop -D "$PG_DATA" -m fast -w 2>/dev/null || true
    sleep 1
    sudo rm -f "$PG_DATA/postmaster.pid" 2>/dev/null || true
    if ! sudo -u postgres "$PG_BIN/pg_ctl" start -D "$PG_DATA" \
            -o "-c config_file=$PG_CONF -p $PGPORT" \
            -l "$PG_LOG" -w 2>"$build_log"; then
        echo "POSTGRES FAILED (restart)"
        cat "$build_log"
        exit 1
    fi
fi
# Wait until accepting connections (max ~5s)
for _ in {1..25}; do
    if sudo -u postgres "$PG_BIN/psql" -p "$PGPORT" -c 'select 1' >/dev/null 2>&1; then
        break
    fi
    sleep 0.2
done

#-- 4. Drop the regression DB to ensure a clean slate ---------------------
sudo -u postgres "$PG_BIN/psql" -p "$PGPORT" \
    -c "DROP DATABASE IF EXISTS contrib_regression" >/dev/null 2>&1 || true

#-- 5. Run regression ------------------------------------------------------
test_log="$(mktemp)"
trap 'rm -f "$build_log" "$test_log"' EXIT

# `make test` writes results/ in the project dir; need a writable cwd for postgres uid.
chmod -R a+w "$PROJECT/test" 2>/dev/null || true

REGRESS_ARG=""
if [[ -n "$REGRESS" ]]; then
    REGRESS_ARG="REGRESS='$REGRESS'"
fi

sudo -u postgres bash -c "
    cd '$PROJECT'
    PGPORT='$PGPORT' PATH='$PG_BIN':\$PATH make test $REGRESS_ARG
" >"$test_log" 2>&1
test_rc=$?

# Parse output
total="$(grep -cE '^(ok|not ok) +[0-9]+' "$test_log")"
failed_tests="$(grep -E '^not ok' "$test_log" | sed -E 's/^not ok +[0-9]+ +- +([^[:space:]]+).*/\1/' | tr '\n' ',' | sed 's/,$//; s/,/, /g')"

if [[ $test_rc -eq 0 ]]; then
    echo "OK $total/$total"
    exit 0
fi

failed_count="$(grep -cE '^not ok' "$test_log")"
echo "FAILED ($failed_count): $failed_tests"

# Print first ~40 lines of diff for the first failing test
if [[ -s "$PROJECT/test/regression.diffs" ]]; then
    echo "--- regression.diffs (first 40 lines) ---"
    head -40 "$PROJECT/test/regression.diffs"
fi
exit 1
