"""
Where closed KPI windows go.

KpiStore keeps them in SQLite (one row per feed per window) and can export
a CSV that Power BI / Excel read directly. Writes are upserts, so
replaying the same window (e.g. after a consumer restart) just
overwrites it instead of double counting.
"""
import csv
import sqlite3
from dataclasses import fields
from pathlib import Path

from .kpi import KpiRecord

COLUMNS = [f.name for f in fields(KpiRecord)]

DDL = """
CREATE TABLE IF NOT EXISTS dq_kpi_window (
    feed              TEXT    NOT NULL,
    window_start      TEXT    NOT NULL,
    window_end        TEXT    NOT NULL,
    events            INTEGER NOT NULL,
    completeness_pct  REAL,
    error_rate_pct    REAL,
    p50_lag_s         REAL,
    p95_lag_s         REAL,
    max_lag_s         REAL,
    sla_breach_pct    REAL,
    dropped_late      INTEGER,
    top_missing       TEXT,
    top_error         TEXT,
    PRIMARY KEY (feed, window_start)
);

CREATE TABLE IF NOT EXISTS dq_alert (
    feed          TEXT NOT NULL,
    metric        TEXT NOT NULL,
    status        TEXT NOT NULL,
    severity      TEXT NOT NULL,
    window_start  TEXT NOT NULL,
    value         REAL,
    expected      TEXT,
    message       TEXT,
    PRIMARY KEY (feed, metric, status, window_start)
);
"""

ALERT_COLUMNS = ["feed", "metric", "status", "severity", "window_start", "value", "expected", "message"]


class KpiStore:
    def __init__(self, path="output/dq_kpi.db"):
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(DDL)

    def write(self, records):
        if not records:
            return 0
        placeholders = ",".join("?" * len(COLUMNS))
        self.conn.executemany(
            f"INSERT OR REPLACE INTO dq_kpi_window ({','.join(COLUMNS)}) VALUES ({placeholders})",
            [tuple(r.to_dict()[c] for c in COLUMNS) for r in records],
        )
        self.conn.commit()
        return len(records)

    def write_alerts(self, alerts):
        if not alerts:
            return 0
        self.conn.executemany(
            f"INSERT OR REPLACE INTO dq_alert ({','.join(ALERT_COLUMNS)}) "
            f"VALUES ({','.join('?' * len(ALERT_COLUMNS))})",
            [tuple(a.to_dict()[c] for c in ALERT_COLUMNS) for a in alerts],
        )
        self.conn.commit()
        return len(alerts)

    def alerts(self, feed=None):
        sql, args = "SELECT * FROM dq_alert", ()
        if feed:
            sql, args = sql + " WHERE feed = ?", (feed,)
        return [dict(r) for r in self.conn.execute(sql + " ORDER BY window_start", args).fetchall()]

    def recent(self, feed, limit=10):
        """Most recent windows for a feed, newest first."""
        rows = self.conn.execute(
            "SELECT * FROM dq_kpi_window WHERE feed = ? ORDER BY window_start DESC LIMIT ?",
            (feed, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def count(self):
        return self.conn.execute("SELECT COUNT(*) FROM dq_kpi_window").fetchone()[0]

    def export_csv(self, path="output/dq_kpi_window.csv"):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        rows = self.conn.execute(
            f"SELECT {','.join(COLUMNS)} FROM dq_kpi_window ORDER BY window_start, feed"
        ).fetchall()
        with open(path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(COLUMNS)
            w.writerows(tuple(r) for r in rows)
        return path, len(rows)

    def export_alerts_csv(self, path="output/dq_alert.csv"):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        rows = self.conn.execute(
            f"SELECT {','.join(ALERT_COLUMNS)} FROM dq_alert ORDER BY window_start, feed"
        ).fetchall()
        with open(path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(ALERT_COLUMNS)
            w.writerows(tuple(r) for r in rows)
        return path, len(rows)

    def close(self):
        self.conn.close()
