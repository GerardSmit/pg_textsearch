#!/bin/bash
#
# Targeted memory leak check using Valgrind.
# Runs a Postgres backend under Valgrind with a SQL script
# that exercises all major feature paths, then reports leaks.
#
# Usage: ./scripts/valgrind-leak-check.sh
#
set -euo pipefail

PGPORT="${PGPORT:-5434}"
PGDATA="/tmp/valgrind_pg_data"
PGLOG="$PGDATA/logfile"
VALGRIND_LOG="/tmp/valgrind-pg.log"
PG_BIN="$(pg_config --bindir)"
SQL_SCRIPT="/tmp/valgrind_test.sql"

# Postgres valgrind suppression file (suppress known Postgres-core leaks)
SUPP_FILE="/tmp/postgres.supp"
cat > "$SUPP_FILE" << 'SUPP'
{
  pg_locale_init
  Memcheck:Leak
  ...
  fun:pg_locale*
}
{
  guc_malloc
  Memcheck:Leak
  ...
  fun:guc_malloc
}
{
  libpq_gettext
  Memcheck:Leak
  ...
  fun:libpq_gettext
}
{
  dsa_area
  Memcheck:Leak
  ...
  fun:dsa_*
}
{
  pg_shared_memory
  Memcheck:Leak
  ...
  fun:PGSharedMemoryCreate
}
{
  libc_dlopen
  Memcheck:Leak
  ...
  fun:_dl_*
}
{
  openssl
  Memcheck:Leak
  ...
  obj:*/libcrypto*
}
SUPP

# SQL script exercising all feature paths
cat > "$SQL_SCRIPT" << 'SQL'
-- Exercise every major feature path for leak detection

CREATE EXTENSION IF NOT EXISTS pg_textsearch;

-- Basic single-column
CREATE TABLE vg_test(id serial, content text);
INSERT INTO vg_test(content)
SELECT 'the quick brown fox jumps over lazy dog number ' || i
FROM generate_series(1, 200) i;

CREATE INDEX vg_idx ON vg_test USING bm25(content) WITH (text_config='english');

-- Basic query (BMW single-term)
SELECT id FROM vg_test ORDER BY content <@> to_bm25query('fox', 'vg_idx') LIMIT 5;

-- Multi-term query
SELECT id FROM vg_test ORDER BY content <@> to_bm25query('quick brown', 'vg_idx') LIMIT 5;

-- Prefix query
SELECT id FROM vg_test ORDER BY content <@> to_bm25query('qui*', 'vg_idx') LIMIT 5;

-- Phrase query
SELECT id FROM vg_test ORDER BY content <@> to_bm25query('"quick brown"', 'vg_idx') LIMIT 5;

-- Phrase-prefix
SELECT id FROM vg_test ORDER BY content <@> to_bm25query('"quick bro*"', 'vg_idx') LIMIT 5;

-- Spill to create segment
SELECT bm25_spill_index('vg_idx');

-- Query after spill (segment path)
SELECT id FROM vg_test ORDER BY content <@> to_bm25query('fox', 'vg_idx') LIMIT 5;

-- Insert more + second spill (multiple segments)
INSERT INTO vg_test(content)
SELECT 'searching for information retrieval algorithms ' || i
FROM generate_series(1, 200) i;
SELECT bm25_spill_index('vg_idx');

-- BMW across multiple segments
SELECT id FROM vg_test ORDER BY content <@> to_bm25query('fox', 'vg_idx') LIMIT 5;

-- Fuzzy query
SELECT id FROM vg_test ORDER BY content <@> bm25_fuzzy('fxo', 'vg_idx', max_distance => 1) LIMIT 5;

-- Highlighting
SELECT bm25_snippet(content, to_bm25query('fox', 'vg_idx')) FROM vg_test
ORDER BY content <@> to_bm25query('fox', 'vg_idx') LIMIT 3;

SELECT bm25_snippet_positions(content, to_bm25query('fox', 'vg_idx')) FROM vg_test
ORDER BY content <@> to_bm25query('fox', 'vg_idx') LIMIT 3;

-- Multi-column with field scope
CREATE TABLE vg_mc(id serial, title text, body text);
INSERT INTO vg_mc(title, body) VALUES
  ('PostgreSQL full text search', 'BM25 ranking with field weights'),
  ('Quick brown fox', 'The lazy dog sleeps in the sun');

CREATE INDEX vg_mc_idx ON vg_mc USING bm25(title, body) WITH (text_config='english');

-- Field-scoped query
SELECT id FROM vg_mc
ORDER BY (title, body) <@> to_bm25query('title:fox', 'vg_mc_idx') LIMIT 5;

-- Grouped field scope
SELECT id FROM vg_mc
ORDER BY (title, body) <@> to_bm25query('title:(quick brown)', 'vg_mc_idx') LIMIT 5;

-- Escaped query
SELECT id FROM vg_mc
ORDER BY (title, body) <@> to_bm25query('title\:fox', 'vg_mc_idx') LIMIT 5;

-- Highlighting with field scope
SELECT bm25_snippet(title, to_bm25query('title:fox', 'vg_mc_idx'), field_name => 'title')
FROM vg_mc
ORDER BY (title, body) <@> to_bm25query('title:fox', 'vg_mc_idx') LIMIT 3;

-- VACUUM (alive bitset path)
DELETE FROM vg_test WHERE id <= 50;
VACUUM vg_test;

-- Query after vacuum
SELECT id FROM vg_test ORDER BY content <@> to_bm25query('fox', 'vg_idx') LIMIT 5;

-- Force merge
SELECT bm25_spill_index('vg_idx');

-- Cleanup
DROP TABLE vg_test CASCADE;
DROP TABLE vg_mc CASCADE;
SQL

echo "=== Valgrind Leak Check for pg_textsearch ==="
echo ""

# Check if Postgres is running, stop it
if sudo -u postgres "$PG_BIN/pg_ctl" status -D /var/lib/postgresql/17/main > /dev/null 2>&1; then
    echo "Stopping existing Postgres..."
    sudo -u postgres "$PG_BIN/pg_ctl" stop -D /var/lib/postgresql/17/main -m fast -w 2>/dev/null || true
    sleep 2
fi

# We can't run the full server under Valgrind easily, but we can
# run a single-backend session with pg_regress or psql.
# Use postgres --single for single-backend mode under Valgrind.

# Alternative: run psql connecting to the server, which won't catch
# server-side leaks. Instead, let's use a simpler approach:
# restart the server, run the SQL, then check Postgres logs for
# any ASAN-style reports. Since we don't have ASAN, we'll use
# Valgrind on a single-user postgres session.

echo "Creating temp data directory..."
rm -rf "$PGDATA"
sudo -u postgres "$PG_BIN/initdb" -D "$PGDATA" --auth-local=trust --auth-host=trust -E UTF8 > /dev/null 2>&1

# Configure for pg_textsearch
cat >> "$PGDATA/postgresql.conf" << CONF
shared_preload_libraries = 'pg_textsearch'
port = 55555
log_statement = 'none'
shared_buffers = 128MB
max_connections = 5
CONF

echo "Starting Postgres under Valgrind (this takes ~60s)..."
sudo -u postgres valgrind \
    --leak-check=full \
    --show-leak-kinds=definite,possible \
    --suppressions="$SUPP_FILE" \
    --log-file="$VALGRIND_LOG" \
    --track-origins=yes \
    --num-callers=20 \
    "$PG_BIN/postgres" \
    --single -D "$PGDATA" contrib_regression < /dev/null > /dev/null 2>&1 &

VG_PID=$!

# Wait for Postgres to start (single-user mode starts immediately)
sleep 3

# In single-user mode, we pipe SQL directly. But single-user mode
# is line-oriented and tricky. Instead, let's start the server
# normally under valgrind and connect with psql.

# Kill the single-user attempt
kill $VG_PID 2>/dev/null || true
wait $VG_PID 2>/dev/null || true

echo "Starting Postgres server under Valgrind..."
sudo -u postgres valgrind \
    --leak-check=full \
    --show-leak-kinds=definite,possible \
    --suppressions="$SUPP_FILE" \
    --log-file="$VALGRIND_LOG" \
    --track-origins=yes \
    --num-callers=20 \
    "$PG_BIN/postgres" \
    -D "$PGDATA" > /dev/null 2>&1 &

VG_PID=$!

# Wait for server to be ready
for i in $(seq 1 60); do
    if sudo -u postgres "$PG_BIN/pg_isready" -p 55555 > /dev/null 2>&1; then
        echo "Postgres ready after ${i}s"
        break
    fi
    sleep 1
done

if ! sudo -u postgres "$PG_BIN/pg_isready" -p 55555 > /dev/null 2>&1; then
    echo "ERROR: Postgres failed to start under Valgrind"
    cat "$VALGRIND_LOG" | tail -20
    kill $VG_PID 2>/dev/null || true
    exit 1
fi

# Create database and run test SQL
echo "Running feature exercise SQL..."
sudo -u postgres "$PG_BIN/createdb" -p 55555 contrib_regression 2>/dev/null || true
sudo -u postgres "$PG_BIN/psql" -p 55555 -d contrib_regression -f "$SQL_SCRIPT" > /dev/null 2>&1

echo "Shutting down Postgres (Valgrind will report leaks)..."
sudo -u postgres "$PG_BIN/pg_ctl" stop -D "$PGDATA" -m fast -w 2>/dev/null || true

# Wait for Valgrind to finish
wait $VG_PID 2>/dev/null || true

echo ""
echo "=== Valgrind Results ==="

# Extract leak summary
if [ -f "$VALGRIND_LOG" ]; then
    # Show only pg_textsearch-related leaks (filter by our source files)
    echo ""
    echo "--- Leaks mentioning pg_textsearch ---"
    grep -A 15 "definitely lost\|possibly lost" "$VALGRIND_LOG" | \
        grep -B 5 "pg_textsearch\|tp_\|bm25\|bmw\|memtable\|segment" | head -80 || echo "(none found)"

    echo ""
    echo "--- Leak Summary ---"
    grep "LEAK SUMMARY\|definitely lost\|indirectly lost\|possibly lost\|still reachable\|suppressed" "$VALGRIND_LOG" | tail -10
else
    echo "No Valgrind log found at $VALGRIND_LOG"
fi

# Cleanup
rm -rf "$PGDATA" "$SQL_SCRIPT" "$SUPP_FILE"

# Restart normal Postgres
echo ""
echo "Restarting normal Postgres..."
sudo -u postgres /usr/lib/postgresql/17/bin/pg_ctl start \
    -D /var/lib/postgresql/17/main -o "-p 5434" -l /var/lib/postgresql/17/main/logfile -w 2>/dev/null || true

echo "Done."
