# Power BI dashboard

The pipeline writes two tables that Power BI reads directly:

| File / table | Grain | Used for |
|---|---|---|
| `dq_kpi_window.csv` / `dq_kpi_window` | one row per feed per minute | trends, KPI cards, heat map |
| `dq_alert.csv` / `dq_alert` | one row per alert state change | alert timeline, open alerts |

`samples/` has a 60-minute run with four incidents already in it (pharmacy error spike
08:20-08:24, eligibility volume drop 08:28-08:31, claims late burst 08:33-08:35,
claims error spike 08:45-08:49), so the report can be built without running anything:

```bash
python simulate.py --minutes 60 \
  --error-spike pharmacy:20:24:0.12 --volume-drop eligibility:28:31:0.2 \
  --late-burst claims:33:35:0.4 --error-spike claims:45:49:0.06 \
  --csv samples/dq_kpi_window.csv
```

## 1. Get data

**CSV (simplest):** Home → Get data → Text/CSV → `dq_kpi_window.csv`, then again for
`dq_alert.csv`. For a live consumer use `python consumer.py --csv`, which rewrites both
files on every flush; point Power BI at `output/` and hit Refresh.

**SQLite:** install the SQLite ODBC driver, create a DSN for `output/dq_kpi.db`, then
Get data → ODBC. Same two tables.

In production the same tables would live in Postgres / Snowflake and the report would
use DirectQuery or a scheduled refresh through the gateway.

## 2. Power Query

For `dq_kpi_window`:

1. `window_start`, `window_end` → **Date/Time/Timezone**, then *Transform → Date/Time/Timezone → Local time*
   (or keep UTC and label the axis).
2. `events`, `dropped_late` → Whole number; `*_pct`, `*_lag_s` → Decimal number.
3. Add column `Minute` = `DateTime.Time([window_start])` for the axis.

For `dq_alert`: `window_start` → Date/Time/Timezone, `value` → Decimal number.

Add a small feed dimension (Enter data) so both tables filter together:

| feed | owner | sla_s |
|---|---|---|
| claims | Claims Ops | 120 |
| eligibility | Enrollment | 300 |
| pharmacy | PBM Team | 60 |

Relationships: `Feed[feed]` 1 → * `dq_kpi_window[feed]`, `Feed[feed]` 1 → * `dq_alert[feed]`.

## 3. DAX measures

Percentages are stored per window, so they have to be **weighted by events** when
rolled up - a 2-event window must not count as much as a 120-event one.

```DAX
Total Events = SUM ( dq_kpi_window[events] )

Completeness % =
DIVIDE (
    SUMX ( dq_kpi_window, dq_kpi_window[events] * dq_kpi_window[completeness_pct] ),
    [Total Events]
)

Error Rate % =
DIVIDE (
    SUMX ( dq_kpi_window, dq_kpi_window[events] * dq_kpi_window[error_rate_pct] ),
    [Total Events]
)

SLA Breach % =
DIVIDE (
    SUMX ( dq_kpi_window, dq_kpi_window[events] * dq_kpi_window[sla_breach_pct] ),
    [Total Events]
)

Worst p95 Lag (s) = MAX ( dq_kpi_window[p95_lag_s] )

Dropped Late = SUM ( dq_kpi_window[dropped_late] )

Avg Events / Min = AVERAGE ( dq_kpi_window[events] )

Alerts Fired =
CALCULATE ( COUNTROWS ( dq_alert ), dq_alert[status] = "FIRING" )

Critical Alerts =
CALCULATE ( COUNTROWS ( dq_alert ), dq_alert[status] = "FIRING", dq_alert[severity] = "CRITICAL" )

-- alerts still open at the end of the selected period
Open Alerts =
VAR fired    = CALCULATE ( COUNTROWS ( dq_alert ), dq_alert[status] = "FIRING" )
VAR resolved = CALCULATE ( COUNTROWS ( dq_alert ), dq_alert[status] = "RESOLVED" )
RETURN fired - resolved

-- 1 = green, 2 = amber, 3 = red; used for conditional formatting
Health Score =
SWITCH (
    TRUE (),
    [Completeness %] < 98 || [Error Rate %] > 2, 3,
    [Completeness %] < 99.5 || [Error Rate %] > 1, 2,
    1
)
```

## 4. Report layout

```
┌───────────────────────────────────────────────────────────────────────────┐
│ [Total Events] [Completeness %] [Error Rate %] [Worst p95 Lag] [Open Alerts]│  cards
├──────────────────────────────────────┬────────────────────────────────────┤
│ Error Rate % by Minute (line,        │ Feed health matrix                  │
│ legend = feed) + constant line at 2% │ rows = feed, cols = Minute,         │
│                                      │ values = Error Rate %,              │
├──────────────────────────────────────┤ background = Health Score (rules)  │
│ Events by Minute (line, legend=feed) ├────────────────────────────────────┤
│ - volume drops jump out here         │ Alert log (table): window_start,   │
│                                      │ feed, metric, status, severity,    │
│ p95 Lag by Minute (line)             │ message; status icon via rules     │
└──────────────────────────────────────┴────────────────────────────────────┘
  slicers: feed, severity
```

Tips:

- Matrix conditional formatting → Background color → *Rules* on `Health Score`:
  1 = green `#2E7D32`, 2 = amber `#F9A825`, 3 = red `#C62828`.
- Line chart → Analytics pane → *Constant line* at the feed's threshold (2% errors,
  98% completeness) so breaches are obvious.
- Add `top_error` and `top_missing` as tooltips on the line charts - hovering a spike
  tells you which field broke.
- Turn on *Edit interactions* so clicking an alert row filters the charts to that feed.

## 5. What it should show with the sample data

- Pharmacy error rate jumps from <1% to 2-9% for 08:20-08:23 and the matrix goes red for
  those minutes; two CRITICAL alerts fire at 08:21 and resolve at 08:24.
- Eligibility events drop from ~120 to ~25 per minute at 08:28-08:30 (CRITICAL volume).
- Claims p95 lag spikes from ~4s to 76-121s at 08:30-08:32 and events dip with `dropped_late`
  > 30 at 08:33-08:34 (late backlog, WARN alerts).
- Claims error rate 3-7% at 08:45-08:48 (CRITICAL).
