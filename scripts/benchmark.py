"""Throughput / latency tests. Timings come from the wall clock and etl.StepRun.

  python scripts/benchmark.py data/sim/zones_v3 --scale 10     # one full + one incremental run
  python scripts/benchmark.py data/sim/zones_v3 --stress       # find the limits of the 2-minute target

--stress writes docs/stress_test.txt and docs/stress_test.png:
  A. history size: the simulated CRM copied N times; one full run and one incremental run
     (5 changed inquiries) per size, until the full run passes the 2-minute target
  B. burst size: at a fixed history, N changed rows committed as 1,000-row CRM transactions,
     then one incremental run without the batch cap
  C. the largest burst again, drained with the batch cap; every run has to stay under 2 minutes
"""
import argparse
import sys
import time
from dataclasses import replace
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from crm import crm_exec  # noqa: E402
from load_snapshot import load  # noqa: E402

from etl import db, pipeline  # noqa: E402
from etl.__main__ import init_dw  # noqa: E402
from etl.config import ROOT, Config  # noqa: E402

ASOF = date(2027, 1, 15)
SLA = 120.0      # seconds: designed latency of one run


def steps(cfg, run_id):
    with db.connect(cfg.dw_dsn, cfg) as c:
        return c.execute('SELECT "Step", "DurationSec", "RowsIn", "RowsOut" FROM etl."StepRun" '
                         'WHERE "RunID"=%s AND "Status"=\'success\' ORDER BY "StepRunID"', (run_id,)).fetchall()


def timed(cfg, mode="incremental"):
    t = time.perf_counter()
    run_id = pipeline.run(cfg, ASOF, mode)
    return run_id, time.perf_counter() - t


def small_change(cfg):
    """A couple of inquiries closed: the everyday incremental load."""
    crm_exec('''UPDATE transaction."Inquiry" SET "Status"='Lost', "LostReason"='Price', "ClosedBy"='Sales',
                "ClosedDate"='2027-01-15 10:00' WHERE "InquiryID" IN (SELECT "InquiryID" FROM transaction."Inquiry"
                WHERE "Status"='Contacted' ORDER BY 1 LIMIT 5)''', cfg)


def burst(cfg, n, chunk=1000):
    """n changed rows (chat messages first, then inquiries), one CRM transaction per chunk,
    as the CRM would commit them. The CDC trigger stamps each transaction with its own time."""
    with db.connect(cfg.crm_dsn, cfg, autocommit=True) as c:
        left = n
        for table, key in (("ChatMessage", "ChatMessageID"), ("Inquiry", "InquiryID")):
            ids = [r[key] for r in c.execute(f'SELECT "{key}" FROM transaction."{table}" ORDER BY 1 LIMIT %s', (left,))]
            for i in range(0, len(ids), chunk):
                c.execute(f'UPDATE transaction."{table}" SET "UpdatedAt" = now() WHERE "{key}" = ANY(%s)', (ids[i:i + chunk],))
            left -= len(ids)
            if not left:
                break
    return n - left


def benchmark(zones, scale, cfg):
    counts = load(zones, scale, cfg)
    init_dw(cfg, drop=True)
    full, t_full = timed(cfg, "full")
    small_change(cfg)
    # overlap 0: every row was written seconds ago, so the default 5-minute overlap would
    # re-read the whole CRM and hide the effect of the watermark
    inc, t_inc = timed(replace(cfg, overlap_seconds=0))

    rows = sum(counts.values())
    print(f"\nCRM rows (scale x{scale}): {rows:,}  " + ", ".join(f"{k} {v:,}" for k, v in counts.items()))
    print(f"\n{'step':<22}{'full s':>9}{'rows in':>10}{'rows/s':>10}   {'incr s':>8}{'rows in':>9}")
    inc_steps = {s["Step"]: s for s in steps(cfg, inc)}
    for s in steps(cfg, full):
        i = inc_steps[s["Step"]]
        rps = f'{s["RowsIn"] / float(s["DurationSec"]):,.0f}' if s["RowsIn"] and s["DurationSec"] else "-"
        print(f'{s["Step"]:<22}{float(s["DurationSec"]):>9.3f}{s["RowsIn"] or "-":>10}{rps:>10}   '
              f'{float(i["DurationSec"]):>8.3f}{i["RowsIn"] if i["RowsIn"] is not None else "-":>9}')
    print(f"\nfull run end to end: {t_full:.2f} s ({rows / t_full:,.0f} CRM rows/s)")
    print(f"incremental run after 5 changed inquiries (CRM commit -> visible in the warehouse): {t_inc:.2f} s")
    return rows, t_full, t_inc


def stress(zones, cfg, scales, burst_scale, bursts, cap):
    out, lines = ROOT / "docs", []

    def say(s=""):
        print(s, flush=True)
        lines.append(s)

    say(f"Stress test: latency target {SLA:.0f} s per run. Machine: this laptop, local PostgreSQL 16.\n")
    say("A. History size (simulated CRM copied N times)")
    say(f"{'copies':>7}{'CRM rows':>12}{'full run s':>12}{'incremental s':>15}")
    hist = []
    for n in scales:
        rows, t_full, t_inc = benchmark(zones, n, cfg)
        hist.append((n, rows, t_full, t_inc))
        say(f"{n:>7}{rows:>12,}{t_full:>12.1f}{t_inc:>15.1f}")
        if t_full > SLA:
            break

    say(f"\nB. Burst size (history x{burst_scale}; changes committed as 1,000-row CRM transactions; no cap)")
    say(f"{'changed rows':>13}{'incremental s':>15}{'rows/s':>10}")
    load(zones, burst_scale, cfg)
    init_dw(cfg, drop=True)
    pipeline.run(cfg, ASOF, "full")
    nocap = replace(cfg, overlap_seconds=0, max_rows_per_run=0)
    bur = []
    for n in bursts:
        n = burst(cfg, n)
        _, t = timed(nocap)
        bur.append((n, t))
        say(f"{n:>13,}{t:>15.1f}{n / t:>10,.0f}")

    big = bur[-1][0]
    say(f"\nC. The largest burst ({big:,} changes) again, drained with the batch cap of {cap:,} rows per run"
        " (5-minute overlap, runs spaced as scheduled)")
    say(f"{'run':>4}{'rows read*':>12}{'queued after':>14}{'seconds':>9}")
    burst(cfg, big)
    # production overlap (5 min); runs are spaced like the scheduler spaces them, which we
    # simulate by moving the recorded snapshot time on (see tests/test_backlog.py)
    capped = replace(cfg, max_rows_per_run=cap)
    with db.connect(cfg.dw_dsn, cfg) as c:      # the last run of B was also 'scheduled' earlier
        c.execute('UPDATE etl."Watermark" SET "ExtractedAt" = "ExtractedAt" + interval \'10 minutes\'')
    drain, backlog, k = [], None, 0
    while backlog != 0 and k < 50:
        k += 1
        run_id, t = timed(capped)
        m = next(s for s in steps(cfg, run_id) if s["Step"] == "extract")
        with db.connect(cfg.dw_dsn, cfg) as c:
            backlog = c.execute('SELECT ("Metrics"->>\'backlog\')::int AS b FROM etl."StepRun" WHERE "RunID"=%s '
                                'AND "Step"=\'extract\' AND "Status"=\'success\'', (run_id,)).fetchone()["b"]
        drain.append((k, m["RowsIn"], backlog, t))
        with db.connect(cfg.dw_dsn, cfg) as c:
            c.execute('UPDATE etl."Watermark" SET "ExtractedAt" = "ExtractedAt" + interval \'10 minutes\'')
        say(f"{k:>4}{m['RowsIn']:>12,}{backlog:>14,}{t:>9.1f}")

    say(f"  * rows read = up to {cap:,} changes + the parent inquiries pulled forward with them")
    # a run costs about a + b * history rows + c * changed rows; fit a and b on the
    # incremental runs of A, c on the bursts of B, and report the limits they imply
    import numpy as np
    hx, hy = np.array([h[1] for h in hist], float), np.array([h[3] for h in hist], float)
    b_hist, a_hist = np.polyfit(hx, hy, 1)
    bx, by = np.array([b[0] for b in bur], float), np.array([b[1] for b in bur], float)
    c_burst, a_burst = np.polyfit(bx, by, 1)
    say(f"\nLimits (linear fit; measured up to {hx.max():,.0f} rows of history and {bx.max():,.0f} changes,"
        " beyond that it is an extrapolation)")
    say(f"  incremental run ≈ {a_hist:.1f} s + {b_hist * 1e6:.1f} s per million CRM rows of history")
    say(f"  each changed row adds ≈ {c_burst * 1e6:.1f} s per million changes")
    say(f"  history at which a small incremental run reaches {SLA:.0f} s ≈ {(SLA - a_hist) / b_hist:,.0f} CRM rows")
    say(f"  changes per run that fit in {SLA:.0f} s at history x{burst_scale} ≈ {(SLA - a_burst) / c_burst:,.0f}")
    (out / "stress_test.txt").write_text("\n".join(lines) + "\n")

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.2))
    ax1.plot(hx / 1e6, [h[2] for h in hist], "o-", label="full run (initial load)")
    ax1.plot(hx / 1e6, hy, "s-", label="incremental run (5 changes)")
    ax1.set_xlabel("CRM rows of history (millions)"); ax1.set_title("A. History size")
    ax2.plot(bx / 1e3, by, "o-", label="incremental run, no cap")
    ax2.set_xlabel(f"changed rows in one run (thousands), history x{burst_scale}"); ax2.set_title("B. Burst size")
    for ax in (ax1, ax2):
        ax.axhline(SLA, color="#c0392b", ls="--", lw=1, label="2-minute target")
        ax.set_ylabel("seconds"); ax.set_ylim(bottom=0); ax.legend(frameon=False, fontsize=8)
        ax.spines[["top", "right"]].set_visible(False)
    plt.tight_layout(); plt.savefig(out / "stress_test.png", dpi=150)
    print(f"\nwritten {out / 'stress_test.txt'} and {out / 'stress_test.png'}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("zones")
    p.add_argument("--scale", type=int, default=10)
    p.add_argument("--stress", action="store_true")
    p.add_argument("--scales", default="10,100,250,500,1000,1500,1999")
    p.add_argument("--burst-scale", type=int, default=100)
    p.add_argument("--bursts", default="10000,50000,100000,200000,326000")
    p.add_argument("--cap", type=int, default=100000)
    a = p.parse_args()
    cfg = Config()
    if a.stress:
        stress(a.zones, cfg, [int(x) for x in a.scales.split(",")], a.burst_scale,
               [int(x) for x in a.bursts.split(",")], a.cap)
    else:
        benchmark(a.zones, a.scale, cfg)


if __name__ == "__main__":
    main()
