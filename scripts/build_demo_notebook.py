"""Builds AisleCo_ETL_Pipeline_Demo.ipynb (then execute it with nbconvert, see README).
With --colab it builds AisleCo_ETL_Pipeline_Demo_Colab.ipynb: the same cells plus setup for Google Colab.
Every cell calls the real pipeline; nothing in the notebook re-implements ETL logic."""
import sys
from pathlib import Path

import nbformat as nbf

ROOT = Path(__file__).resolve().parent.parent
COLAB = "--colab" in sys.argv
cells = []
md = lambda s: cells.append(nbf.v4.new_markdown_cell(s.strip()))
code = lambda s: cells.append(nbf.v4.new_code_cell(s.strip()))
# output folded by default in JupyterLab / classic Notebook (click to expand)
code_collapsed = lambda s: cells.append(nbf.v4.new_code_cell(s.strip(), metadata={"collapsed": True, "jupyter": {"outputs_hidden": True}}))

md("""
# Aisle & Co. ETL pipeline demo
MADT 8102 Capstone, Chapter 6: Data Ingestion and ETL Design

Every cell below calls the pipeline in the `etl/` package, the same code that the command line
(`python -m etl run`) and the Airflow DAG `aisleco_daily_etl` run. The notebook does not recompute any
business rule itself. Two PostgreSQL databases are used:

* `aisleco_crm`: the source, OLTP v5 (OLTP v4 plus `UpdatedAt` on every table)
* `aisleco_dw`: the target, DW v7: a star schema of 4 dimensions and 3 facts, plus `stg` (9 staging
  tables), `etl` (7 control tables) and `mart` (11 views)

| Chapter 6 topic | How the pipeline does it | Section |
|---|---|---|
| Extract: full or incremental | Watermark on `UpdatedAt` per table; `--full-refresh` for a full load; deleted rows found from the key list | 4, 5 |
| Extract: parallel, light on the source | One worker per table, all reading one shared snapshot (`pg_export_snapshot`); bulk `COPY`; `UpdatedAt` indexes | 1, 4 |
| Transform: cleaning, joining, aggregation | SQL files in `sql/transform/`: response time from chat messages, staff hours spread over weeks, funnel flags | 2, 4 |
| Transform: personal data | Customer, contact and chat-account tables are never extracted; `CustomerID` is not read (PDPA) | 1, 4 |
| Transform: validation, lineage, metadata | 27 data-quality rules stored in `etl.DqRule`; `_RunID` on every staging row; a manifest per run | 3, 7 |
| Load: bulk, incremental | `COPY` into landing tables; facts merged, writing only the rows whose values changed | 4, 11 |
| Load: integrity | Foreign and unique keys in DW v7; reconciliation after every load | 1, 12 |
| Modular design | 11 steps, one file or function each; the same steps in the CLI and in Airflow | 2 |
| Error handling | Connection retry with backoff, step log `etl.StepRun`, dead-letter queue `etl.RejectedRow`, alerts `etl.Alert` | 7 to 10 |
| Recovery | Resume skips the steps that succeeded; running again changes nothing | 9, 11 |
| Capacity | Batch cap of 100,000 changes per run; the rest waits in the backlog queue in the CRM | 13 |
| Testing | 33 automated tests, a 28,638-value source-to-target check, a stress test against the 2-minute target | 12 to 14 |
| Business problems | Problem 1 (seasonal demand and capacity): staffing decisions run by run. Problem 2 (high inquiry, low conversion): inquiries followed run by run. The KPIs of both on a business year | 5, 6, 12 |
| ETL or ELT | Python extracts and lands the data; the transformations run as SQL inside the warehouse, so strictly ELT | 1 |
""")

code("""
import os, sys, json, subprocess, logging, time
T0 = time.perf_counter()     # the whole notebook is timed against the 2-minute target in section 13
from dataclasses import replace
from datetime import date
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

ROOT = Path.cwd()
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]
from etl import pipeline, db
from etl.config import Config
from etl.__main__ import init_dw
from crm import create_crm, crm_exec
import load_walkthrough

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")   # show alerts only
pd.set_option("display.width", 170); pd.set_option("display.max_columns", 30); pd.set_option("display.max_colwidth", 70)

cfg = Config()          # aisleco_crm -> aisleco_dw, landing files in data/landing
# Everything in this notebook happens within seconds, so the production overlap of 5 minutes
# would re-read every row each time. With 0 the effect of the watermark is visible.
demo = replace(cfg, overlap_seconds=0)

def dw(sql, params=None, c=cfg):
    with db.connect(c.dw_dsn, c) as conn:
        return pd.DataFrame(conn.execute(sql, params).fetchall())

def crm(sql, params=None, c=cfg):
    with db.connect(c.crm_dsn, c) as conn:
        return pd.DataFrame(conn.execute(sql, params).fetchall())

def run_log(run_id, c=cfg):
    df = dw('''SELECT "Step", "Attempt", "Status", "DurationSec" AS sec, "RowsIn", "RowsOut", "Metrics", left("Error", 80) AS error
               FROM etl."StepRun" WHERE "RunID"=%s ORDER BY "StepRunID"''', (run_id,), c)
    keep = ("inserted", "updated", "unchanged", "deleted", "frozen_changed")
    df["changes"] = df.pop("Metrics").map(lambda m: {k: v for k, v in (m or {}).items() if k in keep and v} or "")
    return df

def staged(run_id, c=cfg):
    m = dw('''SELECT "Metrics" FROM etl."StepRun" WHERE "RunID"=%s AND "Step"='stage' AND "Status"='success' ''', (run_id,), c)
    t = pd.DataFrame(m.iloc[0, 0]["tables"]).T
    return t[["extracted", "rejected", "inserted", "updated", "unchanged", "deleted"]]

def week(start="2027-02-08", c=cfg):
    return dw('''SELECT split_part(employee,' ',1) AS employee, role, "TotalAssignedHours" AS hours, "StandardWeeklyHours" AS std_hours,
                        round("UtilizationRate"*100) AS utilization_pct, "OvertimeHours" AS overtime, "ActiveEvents" AS events
                 FROM mart.v_weekly_workload WHERE week_start=%s AND "TotalAssignedHours" > 0 ORDER BY 1''', (start,), c)
print("CRM:", cfg.crm_dsn, "| DW:", cfg.dw_dsn, "| landing:", cfg.landing_dir.relative_to(ROOT))
""")

md("""
## 1. Architecture and schema

The pipeline follows the five data zones of Section 5.5. The CRM is the Collection zone. Extract copies
the changed rows into landing files (Bronze). The stage step checks them and merges them into `stg`
(Silver). SQL builds the dimensions and facts (Gold), and the `mart` views are what the meetings and
reports read (Consumption). The `etl` schema records every run, step, watermark, quality check, rejected
row and alert.

Source and target are separate databases, and nothing is calculated in the CRM. The transformations run
as SQL inside the warehouse after loading, so strictly the design is ELT (Section 6.1). Two queues hold
data that is not loaded yet: the backlog queue (changes beyond the batch cap wait in the CRM) and the
dead-letter queue (rows that failed a rule wait in `etl.RejectedRow`).
""")

code(r'''
INK, INK2, EDGE, BLUE, GOLD = "#0b0b0b", "#52514e", "#9a9893", "#cde2fb", "#fbe7b5"
BRONZE, GREEN, RED = "#f3e3d3", "#dff0e1", "#b03a2e"
fig, ax = plt.subplots(figsize=(18, 8.4)); ax.set_xlim(0, 18); ax.set_ylim(0, 8.4); ax.axis("off")
def box(x, y, w, h, title, body, fc="#fcfcfb", ls="-", size=8.6):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.04,rounding_size=0.08", lw=1.1, ec=EDGE, fc=fc, ls=ls))
    ax.text(x + w / 2, y + h - 0.2, title, ha="center", va="top", fontsize=10.5, fontweight="bold", color=INK)
    ax.text(x + w / 2, y + (h - 0.45) / 2, body, ha="center", va="center", fontsize=size, color=INK2, linespacing=1.45)
def arrow(x1, y1, x2, y2, label="", color=INK2, ls="-"):
    ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle="-|>", mutation_scale=14, lw=1.3, color=color, ls=ls))
    if label:
        ax.text((x1 + x2) / 2, y1 + 0.2, label, ha="center", fontsize=8.5, color=INK2, style="italic")

ZW, ZY, ZH = 3.0, 5.25, 2.25                      # zone boxes (data at rest)
zx = [0.2 + i * 3.65 for i in range(5)]
box(zx[0], ZY, ZW, ZH, "① Collection", "CRM aisleco_crm (OLTP v5)\n9 tables read, no customer data\nUpdatedAt + trigger + index\nbacklog queue: changes after\nthe batch cutoff wait here")
box(zx[1], ZY, ZW, ZH, "② Bronze", "landing files\ndata/landing/run_NNNNN/\none CSV + one key list per table\nmanifest.json (rows, sha256)", fc=BRONZE)
box(zx[2], ZY, ZW, ZH, "③ Silver", "schema stg, 9 tables\ncleaned copy of the CRM tables\nno personal data\n_RunID on every row (lineage)", fc=BLUE)
box(zx[3], ZY, ZW, ZH, "④ Gold", "schemas dim + fact\n4 dimensions, 3 facts\n(star schema: next image)\nsurrogate keys, Unknown = -1", fc=GOLD)
box(zx[4], ZY, ZW, ZH, "⑤ Consumption", "schema mart, 11 views\nweekly operations meeting\nweekly sales review\nmonthly management report", fc=GREEN)
for i, lab in enumerate(["E", "L1", "T + L2", "read"]):
    arrow(zx[i] + ZW, ZY + ZH / 2, zx[i + 1], ZY + ZH / 2, lab)

PW, PY, PH = 3.35, 2.15, 2.55                     # process boxes (what each step does)
steps = [
    ("Extract", "rows with UpdatedAt after the watermark\noverlap: from 5 min before the\nprevious snapshot (late commits)\nbatch cap: 100,000 changes, oldest first\none shared snapshot, 9 tables in\nparallel, bulk COPY to CSV"),
    ("Stage + data quality", "checksum checked against manifest\nCOPY into land.*, row rules\nbad row -> dead-letter queue\n(etl.RejectedRow), rest upserted\nrows deleted in the CRM removed"),
    ("Gate · Transform · Load", "quality gate (set rules):\na failure blocks the run\nSQL transforms build work.*\nMERGE writes only changed rows\nended weeks stay frozen\nreconcile, then the watermark moves"),
    ("Views", "ratios computed here from\ntheir parts (utilization,\nquote share), never stored\nin the facts"),
]
for i, (t, b) in enumerate(steps):
    cx = zx[i] + ZW + 0.325
    box(cx - PW / 2, PY, PW, PH, t, b, ls=(0, (4, 2)), size=8.2)
    ax.plot([cx, cx], [PY + PH, ZY + ZH / 2 - 0.08], ls=":", color=EDGE, lw=1)
ax.text(zx[1] + ZW + 0.325, PY - 0.17, "dead-letter queue: the row is fixed\nin the CRM and loaded by the next run", ha="center", va="top", fontsize=7.8, color=RED)
ax.text(zx[0] + ZW + 0.325, PY - 0.17, "backlog queue: the next run\ncontinues from the cutoff", ha="center", va="top", fontsize=7.8, color=RED)

box(0.2, 0.12, 17.6, 1.45, "Schema etl: pipeline control (logging, checkpoints, metadata)",
    "Run · StepRun (each attempt = checkpoint) · Watermark (+ snapshot time) · DqRule (27 rules) · DqResult · RejectedRow (dead-letter queue) · Alert\n"
    "connection retry with backoff · each step is one transaction and safe to re-run · resume from the last good step · one writer at a time (advisory lock)")
ax.text(9.0, 8.15, "Aisle & Co. ETL: CRM (OLTP v5) to data warehouse (DW v7), across the five data zones of Section 5.5",
        ha="center", fontsize=13, fontweight="bold", color=INK)
plt.savefig(ROOT / "docs" / "etl_architecture.png", dpi=150, bbox_inches="tight"); plt.show()
''')

md("""
The Gold zone, drawn from the schema file `sql/WeddingOrganizer_DW_v7_dbdiagram.sql`, so the picture always matches
the DDL. FactInquiry uses DimDate in four roles. Ratios are not stored: utilization and quote share are
computed in the `mart` views from the columns shown here.
""")

code(r'''
import re
ddl = (ROOT / "sql" / "WeddingOrganizer_DW_v7_dbdiagram.sql").read_text()
tables = {}
for schema, name, body in re.findall(r'CREATE TABLE "(dim|fact)"\."(\w+)" \((.*?)\n\);', ddl, re.S):
    cols = []
    for line in body.strip().splitlines():
        m = re.match(r'\s*"(\w+)" (\w+(?:\(\d+(?:,\d+)?\))?)', line)
        if m:
            cols.append([m.group(1), m.group(2).lower().replace("decimal", "dec"), "PK" if "PRIMARY KEY" in line else ""])
    tables[name] = cols
fks = re.findall(r'ALTER TABLE "fact"\."(\w+)" ADD FOREIGN KEY \("(\w+)"\) REFERENCES "\w+"\."(\w+)"', ddl)
for t, c, _ in fks:
    for col in tables[t]:
        if col[0] == c:
            col[2] = "FK"
views = re.findall(r"CREATE OR REPLACE VIEW mart\.(\w+)", (ROOT / "sql" / "dw_v7_mart_views.sql").read_text())
others = {s: re.findall(rf'CREATE TABLE "{s}"\."(\w+)"', ddl) for s in ("stg", "etl")}

RH = 0.26                                    # row height
fig, ax = plt.subplots(figsize=(17, 13)); ax.set_xlim(0, 17); ax.set_ylim(-0.5, 12.5); ax.axis("off")
geo = {}
def table(name, x, y_top, w, ncol=1, fc="#fcfcfb"):
    cols = tables[name]
    rows = -(-len(cols) // ncol)
    h = 0.42 + rows * RH + 0.1
    ax.add_patch(FancyBboxPatch((x, y_top - h), w, h, boxstyle="round,pad=0.02,rounding_size=0.06", fc=fc, ec=EDGE, lw=1.1, zorder=3))
    ax.add_patch(FancyBboxPatch((x, y_top - 0.42), w, 0.42, boxstyle="round,pad=0.02,rounding_size=0.06", fc=GOLD if name.startswith("Fact") else BLUE, ec=EDGE, lw=1.1, zorder=3))
    ax.text(x + w / 2, y_top - 0.21, name, ha="center", va="center", fontsize=10, fontweight="bold", color=INK, zorder=4)
    cw = w / ncol
    for i, (c, ty, k) in enumerate(cols):
        cx, cy = x + (i // rows) * cw + 0.08, y_top - 0.42 - 0.05 - (i % rows + 0.5) * RH
        ax.text(cx, cy, f"{k:<2} {c}", ha="left", va="center", fontsize=7.4, family="monospace", zorder=4,
                color=INK if k else INK2, fontweight="bold" if k == "PK" else "normal")
        ax.text(cx + cw - 0.16, cy, ty, ha="right", va="center", fontsize=6.6, family="monospace", color=EDGE, zorder=4)
    geo[name] = (x, y_top - h, w, h)
def edge(a, b, label="", side_a="top", side_b="bottom", fa=0.5, fb=0.5, lx=0, ly=0, at=0.5):
    def pt(n, side, f):
        x, y, w, h = geo[n]
        return {"top": (x + w * f, y + h), "bottom": (x + w * f, y), "left": (x, y + h * f), "right": (x + w, y + h * f)}[side]
    p, q = pt(a, side_a, fa), pt(b, side_b, fb)
    ax.add_patch(FancyArrowPatch(p, q, arrowstyle="-|>", mutation_scale=12, lw=1.1, color=INK2, zorder=2))
    if label:
        ax.text(p[0] + (q[0] - p[0]) * at + lx, p[1] + (q[1] - p[1]) * at + ly, label, ha="center", va="center", fontsize=7.6, color=INK2,
                style="italic", zorder=5, bbox=dict(fc="white", ec="none", pad=1.2))

table("DimDate", 3.6, 12.2, 9.4, ncol=3)
table("FactInquiry", 0.2, 9.6, 4.6)
table("FactEventBooking", 6.2, 9.6, 4.4)
table("FactEmployeeWeeklyWorkload", 12.2, 9.6, 4.6)
table("DimInquiryOutcome", 0.4, 1.95, 3.9)
table("DimPackage", 5.2, 2.15, 3.6)
table("DimEmployee", 10.3, 2.15, 3.6)
roles = {t: [re.sub(r"(?<=[a-z])(?=[A-Z])", " ", c.removesuffix("DateKey")).lower() for tt, c, d in fks if tt == t and d == "DimDate"] for t in tables}
edge("FactInquiry", "DimDate", f"{len(roles['FactInquiry'])} roles: " + ", ".join(roles["FactInquiry"]), fa=0.6, fb=0.06, lx=-0.4)
edge("FactEventBooking", "DimDate", f"{len(roles['FactEventBooking'])} roles: " + ", ".join(roles["FactEventBooking"]), fb=0.5)
edge("FactEmployeeWeeklyWorkload", "DimDate", "week start", fa=0.4, fb=0.94)
edge("FactEventBooking", "FactInquiry", "InquiryID", side_a="left", side_b="right", fa=0.8, fb=0.92)
edge("FactInquiry", "DimInquiryOutcome", "OutcomeKey", side_a="bottom", side_b="top", fa=0.45)
edge("FactInquiry", "DimPackage", "indicative package", side_a="bottom", side_b="top", fa=0.85, fb=0.25)
edge("FactEventBooking", "DimPackage", "package booked", side_a="bottom", side_b="top", fa=0.3, fb=0.7)
edge("FactInquiry", "DimEmployee", "first responder", side_a="bottom", side_b="top", fa=0.95, fb=0.15, at=0.62)
edge("FactEmployeeWeeklyWorkload", "DimEmployee", "employee", side_a="bottom", side_b="top", fa=0.4, fb=0.8)
strip = (f"Other schemas in aisleco_dw:   stg ({len(others['stg'])} tables, Silver: cleaned CRM copy)   ·   "
         f"etl ({len(others['etl'])} control tables: {', '.join(others['etl'])})   ·   mart ({len(views)} views, Consumption)")
ax.text(8.5, -0.35, strip, ha="center", va="bottom", fontsize=8.2, color=INK2)
ax.text(0.2, 12.35, "Gold zone: DW v7 star schema (4 dimensions, 3 facts)", ha="left", va="center", fontsize=13, fontweight="bold", color=INK)
ax.text(16.8, 12.35, "PK primary key   FK foreign key   columns read from sql/WeddingOrganizer_DW_v7_dbdiagram.sql",
        ha="right", va="center", fontsize=7.6, color=INK2)
plt.savefig(ROOT / "docs" / "dw_schema.png", dpi=150, bbox_inches="tight"); plt.show()
''')

md("""
## 2. Steps

Each step does one job and runs in its own transaction, so a failed step leaves nothing half-written and
can be run again. The arrows are the dependencies Airflow uses. The three dimension steps run in parallel
(DimInquiryOutcome is loaded inside `fact_inquiry`), and the weekly workload fact does not wait for the
inquiry facts.
""")

code("""
deps = pipeline.DEPENDS
layer = {}
def depth(n): return layer.setdefault(n, 0 if not deps[n] else 1 + max(depth(p) for p in deps[n]))
for n in deps: depth(n)
cols = {}
for n, d in layer.items(): cols.setdefault(d, []).append(n)
pos = {n: (d * 1.75, -i * 1.0 + (len(ns) - 1) / 2) for d, ns in cols.items() for i, n in enumerate(ns)}
pos["fact_event_booking"] = (pos["fact_event_booking"][0], pos["fact_inquiry"][1])   # keep the weekly edge clear
fig, ax = plt.subplots(figsize=(15, 3.6)); ax.axis("off")
for n, ups in deps.items():
    for u in ups:
        ax.add_patch(FancyArrowPatch((pos[u][0] + 0.62, pos[u][1]), (pos[n][0] - 0.62, pos[n][1]), arrowstyle="-|>", mutation_scale=11, color=EDGE, lw=1))
for n, (x, y) in pos.items():
    fc = "#fbe7b5" if n.startswith(("dim", "fact")) else "#cde2fb" if n in ("stage", "dq_gate", "reconcile") else "#fcfcfb"
    ax.add_patch(FancyBboxPatch((x - 0.6, y - 0.25), 1.2, 0.5, boxstyle="round,pad=0.02,rounding_size=0.06", fc=fc, ec=EDGE))
    ax.text(x, y, n, ha="center", va="center", fontsize=8.5, color=INK)
ax.set_xlim(-0.8, max(p[0] for p in pos.values()) + 0.8); ax.set_ylim(-1.6, 1.6)
ax.set_title("Pipeline steps and dependencies (same graph as Airflow DAG aisleco_daily_etl)", loc="left", fontsize=11, color=INK)
plt.tight_layout(); plt.savefig(ROOT / "docs" / "etl_dag.png", dpi=150, bbox_inches="tight"); plt.show()
""")

md("""
## 3. Start from empty

Create the CRM and the warehouse. The 27 data-quality rules are stored in `etl.DqRule` when the
warehouse is created.
""")

code("""
create_crm(cfg)
init_dw(cfg, drop=True)
rules = dw('SELECT "RuleName", "TableName", "Kind", "Severity", "Description" FROM etl."DqRule" ORDER BY "RuleID"')
print(rules.groupby(["Kind", "Severity"]).size().rename("rules").to_string()); rules
""")

md("""
## 4. First load: Appendix B, parts 0 to 3

The CRM receives the reference data and three couples: Ploy & Ton (met at a wedding fair, booked),
Nok & Bank (Facebook, went silent) and Mint & Jay (asked for 13 Feb 2027). The first run is dated
23 Nov 2026. There is no watermark yet, so it extracts every row.
""")

code("""
parts = list(load_walkthrough.parts())
for label, title, body in parts[:4]:
    crm_exec(body, cfg); print("CRM <-", label, title)
run1 = pipeline.run(demo, date(2026, 11, 23))
snap = {"Run 1 · 23 Nov": week()}
print(f"\\nrun {run1}: succeeded"); run_log(run1)
""")

code("""
print("What was extracted and how it went into staging (run %d):" % run1); staged(run1)
""")

code("""
folder = cfg.landing_dir / f"run_{run1:05d}"
print("Landing folder (Bronze):", folder.relative_to(ROOT))
for f in sorted(folder.iterdir()): print(f"   {f.name:<36} {f.stat().st_size:>7,} bytes")
m = json.loads((folder / "manifest.json").read_text())
pd.DataFrame(m["tables"]).T[["since", "rows", "keys", "max_updated_at", "sha256"]].assign(sha256=lambda x: x.sha256.str[:12] + "…")
""")

md("""
### Source to target for one inquiry (Ploy & Ton, inquiry 1)

The same inquiry in each zone: the CRM rows, the staging row (`CustomerID` is not extracted) and the
warehouse row. The last two lines recompute two rules by hand and compare them with the warehouse.
""")

code("""
print("SOURCE  CRM transaction.Inquiry"); display(crm('''SELECT "InquiryID","CustomerID","ChannelID","InquiryDate","BudgetRange",
       "ConsultationDate","QuotedDate","QuotedAmount","Status","ClosedBy" FROM transaction."Inquiry" WHERE "InquiryID"=1'''))
chat = crm('''SELECT "SentAt","Direction","SenderType","EmployeeID" FROM transaction."ChatMessage" WHERE "InquiryID"=1 ORDER BY 1''')
print("SOURCE  CRM transaction.ChatMessage"); display(chat)
print("SILVER  stg.Inquiry (CustomerID is not extracted: PDPA)"); display(dw('''SELECT "InquiryID","ChannelID","InquiryDate","BudgetRange",
       "QuotedAmount","Status","_RunID" FROM stg."Inquiry" WHERE "InquiryID"=1'''))
print("GOLD    mart.v_inquiry (fact + DimInquiryOutcome)")
f = dw('''SELECT "ChannelName","ResponseTimeMinutes","ResponseTimeBand","FirstResponderEmployeeKey","FurthestStage","Outcome",
                 "BudgetMidpoint","QuotedAmount","QuoteShareOfBudget","DaysToConsultation","IsConverted"
          FROM mart.v_inquiry WHERE "InquiryID"=1''')
display(f)
first_in = chat.query("SenderType == 'Customer'").SentAt.min()
first_staff = chat.query("SenderType == 'Staff' and SentAt >= @first_in").SentAt.min()
print(f"rule check  response time = first staff reply - first customer message (bot ignored) = "
      f"{(first_staff - first_in).total_seconds() // 60:.0f} min  -> fact {f.ResponseTimeMinutes[0]}")
print(f"rule check  quote share = QuotedAmount / BudgetMidpoint = {float(f.QuotedAmount[0]):,.0f} / {float(f.BudgetMidpoint[0]):,.0f}"
      f" = {float(f.QuotedAmount[0]) / float(f.BudgetMidpoint[0]):.4f}  (computed in the view, never stored)")
""")

md('Week of 8 Feb 2027 after run 1: Beam (coordinator) at 109 % and Arm (event staff) at 120 %, as in Appendix B.4.')
code("week()")

md("""
## 5. Problem 1: the staffing decisions, run by run

Problem 1 in the report is seasonal demand and capacity (Section 2). These runs follow the staffing decisions
for the Valentine's weekend in Appendix B.4 and B.5.

10 Dec: freelance coordinator Fah is hired and given both 14 Feb weddings. Only the changed rows are
extracted: one new employee and two updated assignments.
""")

code("""
crm_exec(parts[4][2], cfg)
run2 = pipeline.run(demo, date(2026, 12, 14)); snap["Run 2 · 14 Dec"] = week()
display(staged(run2).query("extracted > 0"))
week()
""")

md('The overload has moved to Fah: 24 h of 20 h, 120 %. 15 Dec: the 480-guest wedding goes back to Beam.')

code("""
crm_exec(parts[5][2], cfg)
run3 = pipeline.run(demo, date(2026, 12, 15)); snap["Run 3 · 15 Dec"] = week()
display(staged(run3).query("extracted > 0"))
week()
""")

md("""
20 Jan 2027: Nina moves to a 36-hour week. In run 4 (as of 1 Feb) the week of 8 Feb is divided by 36,
while weeks that have already ended keep 44 (history without SCD Type 2, Section 5.4). The weekly load
rewrites only the rows from the as-of week onwards (`updated: 2` below) and leaves the ended weeks alone.
This is intended, so no alert is raised.
""")

code("""
crm_exec(parts[6][2], cfg)
run4 = pipeline.run(demo, date(2027, 2, 1)); snap["Run 4 · 1 Feb"] = week()
display(run_log(run4)[["Step", "Status", "sec", "RowsIn", "RowsOut", "changes"]])
nina = dw('''SELECT week_start, "TotalAssignedHours" AS hours, "StandardWeeklyHours" AS std_hours, round("UtilizationRate"*100) AS pct
             FROM mart.v_weekly_workload WHERE employee LIKE 'Nina%%' AND week_start IN ('2027-01-11','2027-02-08') ORDER BY 1''')
nina
""")

code("""
people = ["Som", "Nina", "Beam", "Fah", "Arm"]
fig, ax = plt.subplots(figsize=(12, 4))
colors = ["#c9c7c1", "#9ec5f0", "#2a78d6", "#0b4fa1"]
for i, (label, df) in enumerate(snap.items()):
    vals = df.set_index("employee").reindex(people)["utilization_pct"].astype(float)
    ax.bar([p + (i - 1.5) * 0.2 for p in range(len(people))], vals.fillna(0), width=0.2, color=colors[i], label=label)
ax.axhline(100, color="#eb6834", lw=1.2, ls="--"); ax.text(4.45, 103, "100 %", color="#eb6834", fontsize=9)
ax.set_xticks(range(len(people)), [f"{p}\\n" + {"Som": "lead planner", "Nina": "lead planner", "Beam": "coordinator", "Fah": "freelance coord.", "Arm": "event staff"}[p] for p in people])
ax.set_ylabel("Utilization, week of 8 Feb (%)"); ax.legend(frameon=False, ncol=4, loc="upper left")
ax.set_title("What each ETL run showed management about the Valentine's week", loc="left", fontsize=12)
for s in ("top", "right"): ax.spines[s].set_visible(False)
plt.tight_layout(); plt.savefig(ROOT / "docs" / "valentines_week_by_run.png", dpi=150); plt.show()
""")

code("""
print("Watermarks after run 4 (incremental extraction starts here next time):")
dw('SELECT "SourceTable", "LastUpdatedAt", "LastRunID" FROM etl."Watermark" ORDER BY 1')
""")

md("""
## 6. Problem 2: following the inquiries run by run

Problem 2 in the report is high inquiry, low conversion: nobody can say where couples drop out, which
channels and packages convert, or whether price, response time, date or scope is the main reason
(Section 2). This section replays the three inquiries of Appendix B.1 to B.3 one business day at a time,
with an incremental run after each day, and shows what the warehouse knows after each run.

It uses separate databases (`aisleco_crm_p2`, `aisleco_dw_p2`) and the same SQL as
`AisleCo_Lean_Walkthrough.sql`, cut at its `-- @step` date markers.
""")

code(r"""
from load_walkthrough import steps
p2 = replace(cfg, crm_dsn="dbname=aisleco_crm_p2", dw_dsn="dbname=aisleco_dw_p2",
             landing_dir=ROOT / "data" / "landing_p2", overlap_seconds=0)
days = steps()
create_crm(p2); init_dw(p2, drop=True)
COUPLE = {1: "Ploy & Ton", 2: "Nok & Bank", 3: "Mint & Jay"}
p2_runs = {}

def p2_day(i):
    d, sql = days[i]
    crm_exec(sql, p2)
    rid = pipeline.run(p2, d)
    ext = staged(rid, p2).query("extracted > 0")["extracted"]
    print(f"run {rid}, as of {d:%d %b %Y}. Rows extracted: " + ", ".join(f"{t} {n}" for t, n in ext.items()))
    inq = dw('''SELECT "InquiryID", "ChannelName" AS channel, "ResponseTimeMinutes" AS response_min,
                       "ResponseTimeBand" AS band, "FurthestStage" AS stage, "Outcome" AS outcome,
                       "LostReason" AS lost_reason, "ClosedBy" AS closed_by, "QuoteShareOfBudget" AS quote_share,
                       "DaysToConvert" AS days_to_convert
                FROM mart.v_inquiry ORDER BY 1''', c=p2)
    inq.insert(0, "couple", inq.pop("InquiryID").map(COUPLE))
    funnel = dw('''SELECT count(*) AS inquiries, sum("IsConsulted") AS consulted, sum("IsQuoted") AS quoted,
                          sum("IsConverted") AS booked, sum("IsOpen") AS still_open, sum("IsLost") AS lost,
                          round(sum("IsConverted")::numeric / nullif(count(*) - sum("IsOpen"), 0), 3) AS conversion_of_decided
                   FROM mart.v_inquiry''', c=p2)
    p2_runs[f"{d:%d %b}"] = inq.outcome.value_counts()
    display(inq.fillna("")); display(funnel)

print("Business days in the script:", ", ".join(f"{d:%d %b %Y}" for d, _ in days))
""")

md("""
14 Mar 2026: Ploy and Ton fill the tablet form at the Wedding Fair booth (budget 500k to 1M, wedding on
16 Jan 2027, Full Planning). Ploy scans the booth QR code and writes on LINE at 14:20, and May replies at
14:31. The CRM's reference data and the four Valentine's weddings already booked are loaded in the same run.
""")
code("p2_day(0)")

md("""
20 and 21 Mar: Nok asks about Partial Planning on Facebook at 21:40. The bot answers at 21:41 with the
opening hours. May replies at 10:05 the next morning and sends the package sheet; Nok does not write again.
The response time runs from 21:40 to May's reply: the bot greeting does not count.
""")
code("p2_day(1)")

md("""
24 Mar: Ploy and Ton had their consultation on 21 Mar, and the quote of 95,000 is sent on 24 Mar. One row
changed in the CRM, so one row is extracted.
""")
code("p2_day(2)")

md("""
2 Apr: Ploy and Ton sign. The event and two staff assignments are created, and Sales closes the inquiry
as converted.
""")
code("p2_day(3)")

md("""
20 Apr: 30 days after Nok's last message, the CRM rule closes the inquiry as lost, reason "No reply",
closed by AutoRule. No reason is invented for her.
""")
code("p2_day(4)")

md("""
20 and 21 Nov: Mint and Jay come from a TikTok link and ask for Saturday 13 Feb 2027, an auspicious date.
May replies in 7 minutes, checks the weekly workload view (Beam already above 100 % that week, two
weddings on the 13th) and offers 20 Feb instead. They decline, because the 13th is the date their monk
gave them. Sales closes the inquiry as lost, reason "Date unavailable". This is where the two problems
meet: a sale lost to capacity, counted as turned-away peak demand.
""")
code("p2_day(5)")

md("""
After the six runs: the outcome of each inquiry run by run, the questions of Appendix B.6 answered from the
warehouse, and a check that loading day by day gives the same inquiry facts as the single load of section 4.
""")

code(r"""
oc = pd.DataFrame(p2_runs).T.reindex(columns=["Open", "Booked", "Lost"]).fillna(0)
fig, ax = plt.subplots(figsize=(10, 3.2))
oc.plot.bar(stacked=True, ax=ax, color=["#c9c7c1", "#2a78d6", "#eb6834"], width=0.6)
ax.set_ylabel("inquiries"); ax.set_xlabel("run (as of)"); ax.tick_params(axis="x", rotation=0)
ax.set_yticks(range(0, int(oc.sum(axis=1).max()) + 1))
ax.set_title("Outcome of each inquiry after each run", loc="left", fontsize=12)
ax.legend(frameon=False, ncol=3, loc="upper left")
for s in ("top", "right"): ax.spines[s].set_visible(False)
plt.tight_layout(); plt.savefig(ROOT / "docs" / "problem2_by_run.png", dpi=150); plt.show()

v = dw('''SELECT i."InquiryID", i."ResponseTimeMinutes" AS rt, i."ResponseTimeBand" AS band, i."LostReason" AS lr,
                 i."ClosedBy" AS cb, i."QuotedAmount" AS qa, i."BudgetMidpoint" AS bm, i."QuoteShareOfBudget" AS qs,
                 i."DaysToConvert" AS dtc, d."FullDate" AS req, d."IsHighSeason" AS hs, d."AuspiciousLevel" AS al
          FROM mart.v_inquiry i JOIN dim."DimDate" d ON d."DateKey" = i."RequestedEventDateKey"''', c=p2).set_index("InquiryID")
ploy, nok, mint = v.loc[1], v.loc[2], v.loc[3]
turned = int(dw('SELECT sum(inquiries_turned_away) AS n FROM mart.v_turned_away_demand', c=p2).n[0])
b6 = pd.DataFrame([
    ("How fast did we answer?",
     f"Ploy {ploy.rt} min ({ploy.band}), Mint {mint.rt} min ({mint.band}), Nok {nok.rt} min ({nok.band}), "
     "counted from the couple's first message, not from the bot", "Is an evening inbox shift worth it?"),
    ("Why did we lose them?", f"Nok: {nok.lr} ({nok.cb}). Mint: {mint.lr} ({mint.cb}). No reason is invented for Nok.",
     "Follow-up before day 30; capacity on peak dates"),
    ("What did Ploy's quote take from her budget?",
     f"{float(ploy.qa):,.0f} / {float(ploy.bm):,.0f} midpoint = {float(ploy.qs):.1%}. Booked {ploy.dtc:.0f} days after the fair.",
     "At what fee share do couples walk away on price?"),
    ("How much peak demand did we turn away?",
     f"{turned} couple, for {mint.req:%d %b %Y} (high season: {mint.hs}, auspicious level: {mint.al})",
     "Freelance cover or a peak-date premium"),
], columns=["question (Appendix B.6)", "answer from the warehouse", "decision it supports"])
with pd.option_context("display.max_colwidth", 140):
    display(b6)

keys = ["InquiryFactKey", "OutcomeKey", "IndicativePackageKey", "FirstResponderEmployeeKey"]   # surrogate keys differ by load order
a = dw('SELECT * FROM mart.v_inquiry ORDER BY "InquiryID"', c=p2).drop(columns=keys)
b = dw('SELECT * FROM mart.v_inquiry ORDER BY "InquiryID"').drop(columns=keys)
print("Same inquiry facts as the single load of section 4:", a.astype(str).equals(b.astype(str)))
""")

md("""
## 7. Data quality: a bad row goes to the dead-letter queue and returns when fixed

A staff reply is logged without the staff member, which the CRM does not enforce. Rule
`chat_staff_without_employee` rejects the row: it is stored with its full content in `etl.RejectedRow`,
the rest of the load continues, and a warning alert is written.
""")

code("""
crm_exec('''INSERT INTO transaction."ChatMessage"("ChatMessageID","CustomerAccountID","InquiryID","PlatformMessageID",
            "SentAt","Direction","SenderType","EmployeeID") VALUES (900,3,3,'L-0900','2026-11-22 10:00','Out','Staff',NULL)''', cfg)
run5 = pipeline.run(demo, date(2027, 2, 1))
display(staged(run5).query("extracted > 0"))
dw('''SELECT d."RunID", r."RuleName", d."SourceTable", d."SourceKey", d."Payload", d."ResolvedRunID"
      FROM etl."RejectedRow" d JOIN etl."DqRule" r USING ("RuleID")''')
""")

md('The record is corrected in the CRM. Its `UpdatedAt` changes, so the next run extracts it, loads it and marks the dead-letter row as resolved.')

code("""
crm_exec('UPDATE transaction."ChatMessage" SET "EmployeeID"=5 WHERE "ChatMessageID"=900', cfg)
run6 = pipeline.run(demo, date(2027, 2, 1))
dw('''SELECT d."RunID" AS rejected_in_run, d."SourceKey", d."ResolvedRunID" AS loaded_in_run FROM etl."RejectedRow" d''')
""")

md("""
## 8. The quality gate stops a systematic problem

An upstream change starts sending a new sender type (`AutoReply`) on every message. 30 of 30 new rows are
rejected, above the 5 % limit, so the `dq_gate` step stops the run. No dimension or fact is changed and
the watermark does not move, so these rows are read again once the cause is fixed.
""")

code("""
before = dw('SELECT count(*) AS n, sum("ResponseTimeMinutes") AS s FROM fact."FactInquiry"')
crm_exec('''INSERT INTO transaction."ChatMessage"("CustomerAccountID","InquiryID","PlatformMessageID","SentAt","Direction","SenderType")
            SELECT 3, 3, 'X-'||k, '2026-11-22 10:00'::timestamp + k * interval '1 minute', 'Out', 'AutoReply' FROM generate_series(1,30) k''', cfg)
try:
    pipeline.run(demo, date(2027, 2, 1))
except Exception as e:
    print(type(e).__name__, "->", e)
run7 = int(dw('SELECT max("RunID") FROM etl."Run"').iloc[0, 0])
display(run_log(run7)[["Step", "Status", "error"]])
print("FactInquiry before / after:", before.values.tolist(), dw('SELECT count(*) AS n, sum("ResponseTimeMinutes") AS s FROM fact."FactInquiry"').values.tolist())
""")

md('The cause is fixed upstream and the bad rows are removed from the CRM. The next run removes them from staging, closes their dead-letter rows because the keys no longer exist, and succeeds.')

code("""
crm_exec('''DELETE FROM transaction."ChatMessage" WHERE "SenderType"='AutoReply' ''', cfg)
run8 = pipeline.run(demo, date(2027, 2, 1))
dw('SELECT "RunID", "Status", "Message" FROM etl."Run" WHERE "RunID" >= %s ORDER BY 1', (run7,))
""")

md("""
## 9. A crash during the load, then resume from the checkpoint

Fah's assignment grows from 16 h to 20 h. The `ETL_FAIL_AT` test hook makes `fact_weekly_workload` fail
just before it commits. The step's transaction is rolled back, so nothing is half-written. `resume` skips
the steps that already succeeded and finishes the run.

The extra planning hours also fall in four January weeks that have already ended. Those rows stay frozen
and the run reports them as a late change. The warning appears twice in the log below because the failed
attempt raised it too; that attempt was rolled back, so `etl.Alert` holds it once. Later runs record the
same count in `etl.DqResult` without alerting again.
""")

code("""
crm_exec('UPDATE transaction."EventEmployeeAssignment" SET "WorkloadHours"=20 WHERE "EventID"=104 AND "EmployeeID"=6', cfg)
try:
    pipeline.run(replace(demo, fail_at="fact_weekly_workload"), date(2027, 2, 1))
except RuntimeError as e:
    print("crashed:", e)
run9 = int(dw('SELECT max("RunID") FROM etl."Run"').iloc[0, 0])
pipeline.resume(demo, run9)
display(run_log(run9)[["Step", "Attempt", "Status", "sec", "changes", "error"]])
with pd.option_context("display.max_colwidth", 130):     # full text, so "late change" is visible
    display(dw('''SELECT "RunID", "Severity", split_part("Message", ';', 1) AS "Message" FROM etl."Alert" ORDER BY "AlertID"'''))
""")

md("""
## 10. Extract fails: the CRM cannot be reached

The connection is retried with exponential backoff, which covers short outages. After the last attempt the
run fails in the extract step, before anything is written. Staging and the warehouse are unchanged, an
error alert is stored, and the watermarks do not move, so the next run reads the same rows. The WARNING
and ERROR lines are the pipeline's log messages.
""")

code("""
wm_before = dw('SELECT "SourceTable", "LastUpdatedAt" FROM etl."Watermark" ORDER BY 1')
down = replace(demo, crm_dsn="host=127.0.0.1 port=1 dbname=aisleco_crm", connect_retries=3, retry_base_seconds=1)
try:
    pipeline.run(down, date(2027, 2, 1))
except ConnectionError as e:
    print("run failed:", e)
failed = int(dw('SELECT max("RunID") FROM etl."Run"').iloc[0, 0])
display(dw('SELECT "RunID", "Status", "Message" FROM etl."Run" WHERE "RunID"=%s', (failed,)))
display(run_log(failed)[["Step", "Attempt", "Status", "error"]])
display(dw('SELECT "Severity", "Message" FROM etl."Alert" WHERE "RunID"=%s', (failed,)))
print("watermarks unchanged:", wm_before.equals(dw('SELECT "SourceTable", "LastUpdatedAt" FROM etl."Watermark" ORDER BY 1')))
""")

md("""
### Where each stage can fail, and what the pipeline does

| Stage | Failure | What happens | Where it is logged | How to recover |
|---|---|---|---|---|
| E: extract | CRM down or network outage | Retry with backoff, then the run fails before anything is written (section 10) | `etl.StepRun`, `etl.Run`, `etl.Alert` | Restore the connection and run again; the watermark has not moved |
| E to L: landing | File damaged or changed after extract | Checked against the manifest checksum; the stage step refuses it (test `damaged_landing_file`) | `etl.StepRun.Error`, `etl.Alert` | Run again; the files are extracted again |
| T: row rules | One bad row (missing date, unknown status, orphan) | The row goes to the dead-letter queue `etl.RejectedRow`; the rest loads (section 7) | `etl.RejectedRow`, `etl.DqResult`, warning alert | Correct it in the CRM; the next run loads it and marks it resolved |
| T: quality gate | Systematic problem (reject rate above 5 %, orphans in staging) | The run stops before any dimension or fact changes (section 8) | `etl.DqResult`, error alert | Fix the cause upstream and run again |
| L: load | Crash inside a step | The step's transaction is rolled back; `resume` skips the finished steps (section 9) | `etl.StepRun` (attempt 1 failed, attempt 2 succeeded) | `python -m etl resume RUN_ID` |
| L: reconcile | Target does not match source | The run is marked failed and the watermarks do not move | `etl.DqResult`, error alert | Investigate; the next run reads the same rows again |
| Volume | More changes than one run may take | Batch cap; the rest waits in the backlog queue (section 13) | Warning alert, `backlog` in `etl.StepRun.Metrics` | None; the following runs drain it |
""")

md("""
## 11. Idempotency: running again changes nothing

Same CRM, same as-of date. Every row is compared and none is written, so surrogate keys stay the same and a
retry cannot duplicate data. This run uses the production 5-minute overlap, so recent rows are read again;
all of them come out `unchanged`. `frozen_changed` is the late change from section 9, still frozen.
""")

code("""
run10 = pipeline.run(cfg, date(2027, 2, 1))      # production overlap (5 min) -> rows are re-read, none rewritten
log = run_log(run10); display(log[["Step", "RowsIn", "RowsOut", "changes"]])
print("Staging:"); staged(run10)[["extracted", "inserted", "updated", "unchanged", "deleted"]]
""")

md("""
## 12. Source-to-target check, and the KPIs of both problems on a business year

The v3 notebook simulates a business year (619 inquiries, 2,652 chat messages, 345 staff assignments) and
computes its own Gold zone in pandas. The pipeline loads the same simulated CRM into separate databases
(`aisleco_crm_sim`, `aisleco_dw_sim`), and every value of the three facts is compared.
""")

code("""
from load_snapshot import load
from crosscheck import compare_with_gold
ZONES = ROOT / "data" / "sim" / "zones_v3"
sim = replace(cfg, crm_dsn="dbname=aisleco_crm_sim", dw_dsn="dbname=aisleco_dw_sim", landing_dir=ROOT / "data" / "landing_sim")
print("CRM rows loaded:", load(ZONES, 1, sim))
init_dw(sim, drop=True)
sim_run = pipeline.run(sim, date(2027, 1, 15), "full")
checks, fails = compare_with_gold(ZONES, sim)
print(f"\\nvalues compared: {checks:,}   differences: {len(fails)}")
run_log(sim_run, sim)[["Step", "sec", "RowsIn", "RowsOut"]]
""")

md("""
### The KPIs of both problems on the simulated year

Every KPI of Section 2.1, read from the `mart` views of the simulated year. The sentence under each table is
computed from the same numbers. The data is simulated: it shows that the warehouse computes what Section 2.1
specifies, not what Aisle & Co.'s real figures are (Section 5.6).

#### Problem 2: high inquiry, low conversion
""")

code(r"""
ch = dw('SELECT * FROM mart.v_funnel_by_channel ORDER BY inquiries DESC', c=sim); display(ch)
big = ch[ch.inquiries >= 30].sort_values("conversion_rate_decided")
print(f"Conversion of decided inquiries runs from {float(big.conversion_rate_decided.iloc[0]):.1%} ({big.ChannelName.iloc[0]}) "
      f"to {float(big.conversion_rate_decided.iloc[-1]):.1%} ({big.ChannelName.iloc[-1]}) among channels with 30 or more inquiries.")
mo = dw('''SELECT d."YearMonth" AS inquiry_month, count(*) AS inquiries, sum(i."IsConverted") AS booked,
                  sum(i."IsOpen") AS still_open,
                  round(sum(i."IsConverted")::numeric / nullif(count(*) - sum(i."IsOpen"), 0), 3) AS conversion_of_decided
           FROM mart.v_inquiry i JOIN dim."DimDate" d ON d."DateKey" = i."InquiryDateKey" GROUP BY 1 ORDER BY 1''', c=sim)
display(mo)
print(f"{int(mo.still_open.sum())} inquiries are still open, {int(mo.still_open.tail(2).sum())} of them from the last two months. "
      "They are kept apart because they have not had time to convert.")
""")

code(r"""
pk = dw('SELECT * FROM mart.v_conversion_by_package ORDER BY inquiries DESC', c=sim); display(pk)
print("Conversion by the package the couple asked about: " +
      ", ".join(f"{r.PackageFamily} {float(r.conversion_rate):.1%}" for r in pk.itertuples()) + ".")
rt = dw('''SELECT * FROM mart.v_response_vs_conversion
           ORDER BY array_position(array['<1h','1-24h','>24h','Not logged','In person'], "ResponseTimeBand"::text)''', c=sim)
display(rt)
r = rt.set_index("ResponseTimeBand")
print(f"Answered within 1 hour: {float(r.conversion_rate['<1h']):.1%} converted (median reply {r.median_minutes['<1h']:.0f} min). "
      f"Answered in 1 to 24 hours: {float(r.conversion_rate['1-24h']):.1%} (median {r.median_minutes['1-24h']:.0f} min).")
""")

code(r"""
sd = dw('SELECT * FROM mart.v_stage_dropoff', c=sim); display(sd)
s = sd.iloc[0]
print(f"Of {s.inquiries} inquiries, {s.consulted / s.inquiries:.0%} reached a consultation, {s.quoted / s.inquiries:.0%} a quote "
      f"and {s.booked / s.inquiries:.0%} a booking. A booking took {s.avg_days_to_convert} days from the first contact on average.")
lr = dw('SELECT * FROM mart.v_lost_reasons ORDER BY lost DESC', c=sim); display(lr)
print(f"{float(lr[lr.ClosedBy == 'AutoRule'].share_of_all_lost.sum()):.0%} of lost inquiries were closed by the 30-day rule "
      "(the couple went silent); the rest carry a reason given by Sales.")
qs = dw('SELECT * FROM mart.v_quote_share ORDER BY outcome', c=sim); display(qs)
q = qs.set_index("outcome")
print(f"Median quote share of budget: {float(q.median_quote_share['Booked']):.1%} for booked couples, "
      f"{float(q.median_quote_share['Lost on price']):.1%} for couples lost on price.")
""")

md("#### Problem 1: seasonal demand and capacity")

code(r"""
pa = dw('SELECT * FROM mart.v_peak_acceptance', c=sim); display(pa)
p = pa.iloc[0]
print(f"Of {p.peak_inquiries} inquiries for a high-season or auspicious date, {float(p.acceptance_rate):.1%} became a booking; "
      f"{p.lost_date_unavailable} were lost because the date was unavailable.")
ta = dw('SELECT * FROM mart.v_turned_away_demand ORDER BY inquiries_turned_away DESC, requested_month LIMIT 6', c=sim); display(ta)
sh = dw('SELECT * FROM mart.v_staff_hours_per_wedding ORDER BY avg_total_hours DESC', c=sim); display(sh)
print(f"Staff hours per wedding run from {sh.avg_total_hours.min()} to {sh.avg_total_hours.max()} hours on average, "
      "by package, complexity and guest count.")
wk = dw('''SELECT week_start, count(*) FILTER (WHERE "TotalAssignedHours" > "StandardWeeklyHours") AS people_over_100pct,
                  round(max("UtilizationRate") * 100) AS highest_utilization_pct, sum("OvertimeHours") AS overtime_hours
           FROM mart.v_weekly_workload GROUP BY 1
           ORDER BY people_over_100pct DESC, highest_utilization_pct DESC LIMIT 6''', c=sim); display(wk)
print(f"The busiest week starts {wk.week_start[0]:%d %b %Y}: {wk.people_over_100pct[0]} people above 100 %, "
      f"{float(wk.overtime_hours[0]):.0f} overtime hours.")
""")

md("""
## 13. Performance: capacity, latency and the backlog queue

The design target is that one run finishes within 2 minutes, from a CRM commit to the change being visible
in the warehouse. The stress test (`scripts/benchmark.py --stress`) loads millions of rows and takes about
25 minutes, so it is run beforehand and its saved results are shown here. It measures two things, because a
run spends time in two ways:

* History size: Transform rebuilds each fact from staging and the merge then writes only the changed rows,
  so even a small incremental run grows with the history.
* Burst size: Extract and stage handle only the changed rows, so their cost grows with the number of changes.
""")

code(r"""
import re
from IPython.display import Image
st = (ROOT / "docs" / "stress_test.txt").read_text()
print(st); display(Image(filename=str(ROOT / "docs" / "stress_test.png")))
hist = re.findall(r"^\s*(\d+)\s+([\d,]+)\s+([\d.]+)\s+([\d.]+)\s*$", st.split("\nA. ")[1].split("\nB. ")[0], re.M)
bursts = re.findall(r"^\s*([\d,]+)\s+([\d.]+)\s+([\d,]+)\s*$", st.split("\nB. ")[1].split("\nC. ")[0], re.M)
top, ok, big = hist[-1], [h for h in hist if float(h[2]) <= 120][-1], bursts[-1]
print(f"Result: an incremental run took {top[3]} s at {top[1]} CRM rows of history ({top[0]} copies of the simulated year), "
      f"and one run absorbed {big[0]} changes in {big[1]} s. The full load stayed within 2 minutes up to {ok[1]} rows "
      f"({ok[2]} s) and took {top[2]} s at {top[1]} rows; it runs once, outside the target. The limits printed under "
      "'Limits' go beyond the measured points and are a linear extrapolation.")
""")

md("""
### Above the limit: the backlog queue

`ETL_MAX_ROWS_PER_RUN` (100,000 by default) limits the changed transaction rows one incremental run takes,
oldest first. Later changes stay in the CRM behind the watermark, and the next run continues from the
cutoff. Four rules keep this correct:

* a CRM transaction is never split: rows with the same `UpdatedAt` go together;
* a taken row brings its parent rows into the same batch, so no child is rejected as an orphan;
* queued keys are marked `Pending`, so reconciliation does not count them as missing;
* the watermark moves to the cutoff, and the overlap is measured from the previous snapshot, so every run
  moves forward without reading earlier batches again.

On the simulated year from section 12: 1,500 changes committed as 15 CRM transactions, with a cap of 500.
""")

code("""
from benchmark import burst
sim0 = replace(sim, overlap_seconds=0)
burst(sim0, 1500, chunk=100)
capped = replace(sim0, max_rows_per_run=500)
rows, backlog = [], None
while backlog != 0:
    t = time.perf_counter(); rid = pipeline.run(capped, date(2027, 1, 15)); t = time.perf_counter() - t
    m = dw('''SELECT "Metrics" FROM etl."StepRun" WHERE "RunID"=%s AND "Step"='extract' AND "Status"='success' ''', (rid,), sim).iloc[0, 0]
    backlog = m["backlog"]
    rows.append({"run": rid, "chat messages taken": m["per_table"]["ChatMessage"], "inquiries (parents pulled)": m["per_table"]["Inquiry"],
                 "queued after the run": backlog, "seconds": round(t, 2)})
display(pd.DataFrame(rows))
dw('SELECT "RunID", "Message" FROM etl."Alert" WHERE "Message" LIKE %s ORDER BY 1', ("%backlog%",), sim)
""")

md('Run times in this notebook, against the 2-minute target:')

code("""
slowest = lambda c: float(dw('''SELECT max(extract(epoch FROM "FinishedAt" - "StartedAt")) AS s FROM etl."Run"
                                WHERE "Status"='success' ''', c=c).s[0])
pd.DataFrame([
    {"measure": "slowest run, Appendix B (incremental)", "seconds": round(slowest(cfg), 2)},
    {"measure": "slowest run, simulated year (full + capped runs)", "seconds": round(slowest(sim), 2)},
    {"measure": "whole notebook so far (sections 3 to 13)", "seconds": round(time.perf_counter() - T0, 1)},
]).assign(target_s=120, within_target=lambda d: d.seconds <= 120)
""")

md("""
## 14. How we test

The tests are in `tests/` (pytest) and use their own databases (`aisleco_*_test`), so the demo databases
are not touched. Each test builds its own CRM, runs the pipeline, and compares the warehouse with an answer
worked out in advance.

| Test type (brief) | How | Data | Pass means |
|---|---|---|---|
| Functional: transformation rules | One small case per rule with a known answer, e.g. a bot greeting is not a reply (`test_transform_rules.py`) | 14 hand-made CRM cases | The warehouse value equals the expected value |
| Functional: end to end | Appendix B replayed through four incremental runs (`test_walkthrough.py`) | The Appendix B story | Every Appendix B figure reproduced (Beam 109 %, Arm 120 %, Nina 67 % on 36 h and others); incremental equals full refresh; a second run changes nothing |
| Functional: source to target | The same CRM computed twice, independently: in pandas (v3 notebook) and by the pipeline (`test_source_to_target.py`, section 12) | Simulated business year | 28,638 values, 0 differences |
| Performance | History and burst grown until a run passes 2 minutes (`scripts/benchmark.py --stress`, section 13); a burst drained through the batch cap (`test_backlog.py`) | Up to 1,000 copies of the simulated year | Measured limits; no capped run takes more than the cap; the drained warehouse equals a full refresh |
| Error handling and recovery | The failure is injected: closed port, bad row, crash hook (`ETL_FAIL_AT`), damaged file; then the outcome is checked (`test_error_handling.py`) | Appendix B data | Warehouse unchanged or correctly resumed, alert written, watermark not moved, dead-letter row resolved after the fix |

Three tests in full, one per test type. Every test is listed and shown further down. The code is read
from the test files.
""")

code("""
import ast
def show_test(file, name):
    src = (ROOT / "tests" / file).read_text()
    fn = next(n for n in ast.walk(ast.parse(src)) if isinstance(n, ast.FunctionDef) and n.name == name)
    print(f"# tests/{file}\\n" + ast.get_source_segment(src, fn) + "\\n")

print("=== FUNCTIONAL: a bot greeting is not a response ===")
print("# the CRM rows this test is given (inquiry 1, from FIXTURE in the same file):")
fixture = (ROOT / "tests" / "test_transform_rules.py").read_text()
print("\\n".join(l for l in fixture.splitlines() if "(1,1,'m" in l))
print("#  -> couple 21:40, bot 21:41 (ignored), first staff reply 10:05 next day = 745 min, band 1-24h\\n")
show_test("test_transform_rules.py", "test_bot_greeting_is_not_a_response")

print("=== ERROR HANDLING: crash during the load, then resume from the checkpoint ===")
show_test("test_error_handling.py", "test_crash_during_load_then_resume_from_checkpoint")

print("=== CAPACITY: a burst larger than the cap is queued, then drained ===")
show_test("test_backlog.py", "test_burst_is_queued_then_drained")
""")

md("""
### All 33 tests, run now

The Scenario and Expected columns come from each test's docstring in `tests/`. The result comes from this
run of pytest.
""")

code("""
import ast, tempfile, xml.etree.ElementTree as ET
TYPE = {"test_transform_rules.py": "Functional: transformation rules", "test_walkthrough.py": "Functional: end to end (Appendix B)",
        "test_source_to_target.py": "Functional: source to target", "test_backlog.py": "Performance: batch cap / backlog queue",
        "test_error_handling.py": "Error handling and recovery"}
docs = {}
for f in TYPE:
    for fn in ast.parse((ROOT / "tests" / f).read_text()).body:
        if isinstance(fn, ast.FunctionDef) and fn.name.startswith("test_"):
            d = (ast.get_docstring(fn) or "").split("Expected:")
            docs[fn.name] = (f, d[0].replace("Scenario:", "").strip(), d[1].strip() if len(d) > 1 else "")
junit = Path(tempfile.mkdtemp()) / "junit.xml"
t = time.perf_counter()
subprocess.run([sys.executable, "-m", "pytest", "tests", "-q", "--color=no", "-p", "no:cacheprovider", f"--junitxml={junit}"],
               capture_output=True, text=True)
rows = []
for tc in ET.parse(junit).getroot().iter("testcase"):
    f, scen, exp = docs[tc.get("name")]
    bad = [c.tag for c in tc if c.tag in ("failure", "error", "skipped")]
    rows.append({"type": TYPE[f], "test": tc.get("name").removeprefix("test_").replace("_", " "), "scenario": scen,
                 "expected": exp, "result": bad[0].upper() if bad else "PASSED", "s": round(float(tc.get("time")), 2)})
tests = pd.DataFrame(rows)
tests["type"] = pd.Categorical(tests["type"], list(TYPE.values()), ordered=True)   # the brief's order
tests = tests.sort_values("type", kind="stable").reset_index(drop=True)
tests.index += 1
print(f"{(tests.result == 'PASSED').sum()} of {len(tests)} passed in {time.perf_counter() - t:.1f} s:",
      tests.groupby("type", observed=True).size().to_dict())
(tests.style.format({"s": "{:.1f}"}).set_properties(**{"text-align": "left", "white-space": "normal", "vertical-align": "top"})
      .set_properties(subset=["scenario", "expected"], **{"min-width": "260px"})
      .map(lambda v: "color: #1a7f37; font-weight: 600" if v == "PASSED" else "color: #c0392b; font-weight: 600", subset=["result"]))
""")

md("""
### The code of every test

One cell per test file. In JupyterLab the output is folded: click the bar to the left of it to open it.
The first cell also prints the CRM rows the rule tests run on.
""")

CODE_CELL = r'''
src = (ROOT / "tests" / "FILE").read_text()
print("# TITLE\n# tests/FILE\n")
for n in ast.parse(src).body:
    if SHOWFIX and isinstance(n, ast.Assign) and getattr(n.targets[0], "id", "") == "FIXTURE":
        print("FIXTURE = (the CRM rows these tests run on)\n" + ast.get_source_segment(src, n.value) + "\n")
    if isinstance(n, ast.FunctionDef) and (n.name.startswith("test_") or
            any(getattr(getattr(d, "func", d), "attr", "") == "fixture" for d in n.decorator_list)):
        print(ast.get_source_segment(src, n), end="\n\n")
'''
for f, title in [("test_transform_rules.py", "Functional: transformation rules (hand-made CRM data, one rule per test)"),
                 ("test_walkthrough.py", "Functional: end to end, Appendix B of the report"),
                 ("test_source_to_target.py", "Functional: source to target"),
                 ("test_error_handling.py", "Error handling and recovery"),
                 ("test_backlog.py", "Performance: batch cap / backlog queue")]:
    code_collapsed(CODE_CELL.replace("FILE", f).replace("TITLE", title).replace("SHOWFIX", str(f == "test_transform_rules.py")))

md("""
## 15. Airflow and DBeaver

Airflow runs the same steps as tasks (graph in section 2): daily at 02:00 Bangkok time, two retries with
backoff, one run at a time, and an alert callback on failure.

```bash
scripts/airflow.sh standalone          # then open http://localhost:8080  (user admin, password in airflow/standalone_admin_password.txt)
```
In the UI, unpause `aisleco_daily_etl` and use *Trigger DAG w/ config* with `{"asof": "2027-02-01"}`.
Graph shows the steps, Grid shows the run history and retries, and each task's Log shows the step output.

DBeaver: connect to PostgreSQL `localhost:5432`, database `aisleco_dw`, user `pollot`, no password.
Useful to open: `etl.Run`, `etl.StepRun`, `etl.RejectedRow`, `etl.Alert`, the `mart` views, and the
ER diagram of the `dim` and `fact` schemas.
""")

# ---------------------------------------------------------------------------------------------
# Colab variant (python scripts/build_demo_notebook.py --colab): same demo cells, plus setup
# cells that fetch the project and install PostgreSQL 16, and Colab wording where it differs.
# ---------------------------------------------------------------------------------------------
REPO = "hhej/aisleco-etl-pipeline"
if COLAB:
    def at(text):
        return next(i for i, c in enumerate(cells) if text in c.source)

    setup = [
        nbf.v4.new_markdown_cell(f"""### Running this notebook in Google Colab

Choose **Runtime → Run all**. The next two cells fetch the project from GitHub
([`{REPO}`](https://github.com/{REPO})) and install PostgreSQL 16 and the Python driver inside this Colab
session, which takes about 3 minutes. The rest of the notebook then runs in about one minute. Nothing is
installed on your own computer. To run it again from a clean state, use Runtime → Restart session and
run all."""),
        nbf.v4.new_code_cell(f'''# 1. Fetch the project and the Python packages
import os, subprocess, sys
IN_COLAB = "google.colab" in sys.modules
HOME = "/content/aisleco-etl-pipeline"

def sh(cmd):
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if r.returncode:
        raise RuntimeError(f"command failed: {{cmd}}\\n{{r.stdout[-1500:]}}\\n{{r.stderr[-1500:]}}")
    return r.stdout

if IN_COLAB:
    if not os.path.exists(HOME):
        sh("git clone -q https://github.com/{REPO}.git " + HOME)
    os.chdir(HOME)
    sh('pip -q install "psycopg[binary]==3.2.3" "pytest==8.3.4"')
    print("project:", HOME, "| commit", sh("git rev-parse --short HEAD").strip())
else:
    print("local run: using the project folder", os.getcwd())'''),
        nbf.v4.new_code_cell('''# 2. PostgreSQL 16 (the pipeline uses MERGE, which needs version 15 or later)
if IN_COLAB:
    if subprocess.run("pg_lsclusters -h 2>/dev/null | grep -q '^16 '", shell=True).returncode:
        print("installing PostgreSQL 16 from apt.postgresql.org ...")
        sh("apt-get -qq update && apt-get -qq install -y curl ca-certificates > /dev/null")
        sh("install -d /usr/share/postgresql-common/pgdg && curl -fsS -o "
           "/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc https://www.postgresql.org/media/keys/ACCC4CF8.asc")
        sh('. /etc/os-release && echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] '
           'https://apt.postgresql.org/pub/repos/apt $VERSION_CODENAME-pgdg main" > /etc/apt/sources.list.d/pgdg.list')
        sh("apt-get -qq update && DEBIAN_FRONTEND=noninteractive apt-get -qq install -y postgresql-16 > /dev/null")
    sh("pg_ctlcluster 16 main status > /dev/null || pg_ctlcluster 16 main start")
    os.environ["PGPORT"] = sh("pg_lsclusters -h | awk '$1 == 16 {print $3}'").strip()     # used by every connection
    r = subprocess.run(f"su postgres -c \\"psql -p {os.environ['PGPORT']} -c 'CREATE ROLE root SUPERUSER LOGIN'\\"",
                       shell=True, capture_output=True, text=True)
    if r.returncode and "already exists" not in r.stderr:
        raise RuntimeError(r.stderr)
    import psycopg
    with psycopg.connect("dbname=postgres") as c:
        print(c.execute("SELECT version()").fetchone()[0])
else:
    print("local run: using the existing PostgreSQL")'''),
    ]
    cells[1:1] = setup

    cells[at("## 15. Airflow and DBeaver")] = nbf.v4.new_markdown_cell("""## 15. Airflow and DBeaver

Both belong to the local setup and are not used in Colab. Airflow runs the same 11 steps as this notebook
as a daily DAG (`airflow/dags/aisleco_daily_etl.py`, graph in section 2); `README_current_files.md`
explains how to start it locally.""")

    i = at("One cell per test file. In JupyterLab")
    cells[i].source = cells[i].source.replace(
        "One cell per test file. In JupyterLab the output is folded: click the bar to the left of it to open it.",
        "One cell per test file. Each prints the full code of its tests; scroll the output to read it.")

    i = at('stress_test.png')
    cells[i + 1:i + 1] = [
        nbf.v4.new_markdown_cell("The same pipeline on this Colab machine: one full and one incremental run on 10 copies "
                                 "of the simulated year (about 43,000 CRM rows), in separate databases."),
        nbf.v4.new_code_cell('''env = {**os.environ, "ETL_CRM_DSN": "dbname=aisleco_crm_bench", "ETL_DW_DSN": "dbname=aisleco_dw_bench",
       "ETL_LANDING_DIR": str(ROOT / "data" / "landing_bench")}
out = subprocess.run([sys.executable, "scripts/benchmark.py", str(ZONES), "--scale", "10"], env=env, capture_output=True, text=True)
print(out.stdout[-2500:] if out.returncode == 0 else out.stderr[-2500:])'''),
    ]

out = ROOT / ("AisleCo_ETL_Pipeline_Demo_Colab.ipynb" if COLAB else "AisleCo_ETL_Pipeline_Demo.ipynb")
nb = nbf.v4.new_notebook(cells=cells, metadata={"kernelspec": {"name": "python3", "display_name": "Python 3"}})
nbf.write(nb, out)
print("written", out)
