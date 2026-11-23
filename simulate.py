"""
Run the KPI pipeline on simulated traffic and save the results.

    python simulate.py                                   # 30 quiet minutes
    python simulate.py --minutes 60 --error-spike pharmacy:10:15:0.12
    python simulate.py --late-burst claims:20:25:0.4 --volume-drop eligibility:40:45:0.2

Writes output/dq_kpi.db (SQLite) plus output/dq_kpi_window.csv and
output/dq_alert.csv for Power BI.
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from dq_stream.alerts import AlertEngine  # noqa: E402
from dq_stream.config import load_config  # noqa: E402
from dq_stream.simulate import Incident, run_simulation  # noqa: E402
from dq_stream.sink import KpiStore  # noqa: E402


def incident(kind):
    """Parse 'feed:start:end:value' into an Incident."""
    def parse(text):
        try:
            feed, start, end, value = text.split(":")
            start, end, value = int(start), int(end), float(value)
        except ValueError:
            raise argparse.ArgumentTypeError(f"expected feed:start:end:value, got {text!r}") from None
        return Incident(feed=feed, start_min=start, end_min=end, **{kind: value})
    return parse


def main():
    ap = argparse.ArgumentParser(description="simulate feeds and compute windowed DQ KPIs")
    ap.add_argument("--minutes", type=int, default=30)
    ap.add_argument("--rate", type=int, default=120, help="events per minute per feed")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--error-spike", action="append", default=[], type=incident("error_rate"),
                    metavar="FEED:START:END:RATE")
    ap.add_argument("--late-burst", action="append", default=[], type=incident("late_rate"),
                    metavar="FEED:START:END:RATE")
    ap.add_argument("--volume-drop", action="append", default=[], type=incident("volume_factor"),
                    metavar="FEED:START:END:FACTOR")
    ap.add_argument("--db", default="output/dq_kpi.db")
    ap.add_argument("--csv", default="output/dq_kpi_window.csv")
    args = ap.parse_args()

    cfg = load_config()
    incidents = args.error_spike + args.late_burst + args.volume_drop
    for inc in incidents:
        if inc.feed not in cfg.feeds:
            ap.error(f"unknown feed {inc.feed!r}")

    records = run_simulation(cfg, minutes=args.minutes, incidents=incidents, seed=args.seed,
                             events_per_min={f: args.rate for f in cfg.feeds})

    engine = AlertEngine(cfg.feeds)
    alerts = [a for r in records for a in engine.evaluate(r)]

    store = KpiStore(args.db)
    store.write(records)
    store.write_alerts(alerts)
    csv_path, rows = store.export_csv(args.csv)
    alert_path, _ = store.export_alerts_csv(str(Path(args.csv).with_name("dq_alert.csv")))

    print(f"{'window':<6} {'feed':<12} {'events':>6} {'complete%':>9} {'error%':>7} "
          f"{'p95 lag':>8} {'sla%':>6} {'dropped':>7}  top problem")
    print("-" * 86)
    for r in records:
        problem = r.top_error or r.top_missing or ""
        print(f"{r.window_start[11:16]:<6} {r.feed:<12} {r.events:>6} {r.completeness_pct:>9} "
              f"{r.error_rate_pct:>7} {r.p95_lag_s:>8} {r.sla_breach_pct:>6} {r.dropped_late:>7}  {problem}")
    print(f"\nAlerts ({len(alerts)}):")
    for a in alerts or []:
        print(f"  {a.window_start[11:16]}  {a.status:<8} {a.severity:<8} {a.message}")
    if not alerts:
        print("  none")
    print(f"\n{rows} KPI rows -> {args.db}, {csv_path}, {alert_path}")
    store.close()


if __name__ == "__main__":
    main()
