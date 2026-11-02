"""Shared SQLite connection settings and schema-aware performance indexes."""
import sqlite3


# PATCH_MARKER_2026-10-28-outcome-db-http-pacer
def connect_database(path, **kwargs):
    connection = sqlite3.connect(path, **kwargs)
    try:
        connection.execute("PRAGMA busy_timeout=5000")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
    except BaseException:
        connection.close()
        raise
    return connection


def ensure_performance_indexes(connection):
    definitions = [
        ("submission_log", ("submitted_at",), "idx_submitted_at"),
        ("code_submission", ("code", "status"), "idx_code_status"),
        ("telegram_inbox", ("status", "created_at"), "idx_inbox_ready"),
        ("durable_inbox", ("status", "created_at"), "idx_durable_inbox_status_created_at"),
        ("account_stats", ("account",), "idx_account_stats_account"),
        ("website_stats", ("domain",), "idx_website_stats_domain"),
        ("website_stats", ("website",), "idx_website_stats_website"),
    ]
    for table, columns, name in definitions:
        available = {row[1] for row in connection.execute(f'PRAGMA table_info("{table}")')}
        if not set(columns) <= available:
            continue
        covered = False
        for index in connection.execute(f'PRAGMA index_list("{table}")').fetchall():
            # A partial index does not cover all rows.
            if index[4]:
                continue
            escaped = index[1].replace('"', '""')
            indexed = tuple(row[2] for row in connection.execute(f'PRAGMA index_info("{escaped}")'))
            if indexed[:len(columns)] == columns:
                covered = True
                break
        if not covered:
            quoted = ", ".join(f'"{column}"' for column in columns)
            connection.execute(f'CREATE INDEX IF NOT EXISTS "{name}" ON "{table}" ({quoted})')
