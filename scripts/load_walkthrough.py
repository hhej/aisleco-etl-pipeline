"""Appendix B, replayed: the CRM receives each part of AisleCo_Lean_Walkthrough.sql as it
happens, and the ETL runs at the four points marked "ETL run" in that script.

  python scripts/load_walkthrough.py            # fresh CRM + DW, then the whole story
"""
import re
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from crm import ROOT, create_crm, crm_exec  # noqa: E402

from etl import pipeline  # noqa: E402
from etl.__main__ import init_dw  # noqa: E402
from etl.config import Config  # noqa: E402

# part label -> as-of date of the ETL run that follows it (None = no run after this part)
RUN_AFTER = {"PART 0": None, "PART 1": None, "PART 2": None, "PART 3": date(2026, 11, 23),
             "PART 4": date(2026, 12, 14), "PART 4b": date(2026, 12, 15), "PART 5": date(2027, 2, 1)}


def parts():
    text = (ROOT / "AisleCo_Lean_Walkthrough.sql").read_text()
    chunks = re.split(r"^-- =+ (PART \w+): ?(.*?) =+$", text, flags=re.M)
    for i in range(1, len(chunks), 3):
        yield chunks[i], chunks[i + 1].strip(), chunks[i + 2]


def steps(last="PART 3"):
    """The inquiry story of parts 0 to `last`, cut at the "-- @step <date>" markers and sorted by
    date: [(as-of date, SQL)]. Part 0 (reference data) goes with the first day."""
    days, before = {}, ""
    for label, _, body in parts():
        head, *rest = re.split(r"^-- @step (\d{4}-\d{2}-\d{2}).*$", body, flags=re.M)
        if rest:
            assert not re.sub(r"--.*", "", head).strip(), f"{label}: SQL before its first @step marker"
        else:
            before += head
        for d, sql in zip(rest[::2], rest[1::2]):
            days[date.fromisoformat(d)] = days.get(date.fromisoformat(d), "") + sql
        if label == last:
            break
    out = sorted(days.items())
    return [(out[0][0], before + out[0][1])] + out[1:]


def main(cfg=None, on_run=None):
    cfg = cfg or Config()
    create_crm(cfg)
    init_dw(cfg, drop=True)
    runs = {}
    for label, title, body in parts():
        crm_exec(body, cfg)
        print(f"CRM  {label}: {title}")
        if RUN_AFTER[label]:
            run_id = pipeline.run(cfg, RUN_AFTER[label])
            runs[label] = run_id
            print(f"ETL  run {run_id} as of {RUN_AFTER[label]}")
            if on_run:
                on_run(label, run_id)
    return runs


if __name__ == "__main__":
    main()
