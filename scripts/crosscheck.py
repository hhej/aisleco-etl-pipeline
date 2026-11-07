"""Source-to-target check on a realistic volume: the pipeline's three facts vs the
notebook's Gold zone, computed independently in pandas from the same simulated CRM.
(Adapted from crosscheck_v3.py: surrogate keys are replaced by OLTP IDs on both sides.)

  python scripts/crosscheck.py data/sim/zones_v3       # loads the snapshot, runs the ETL, compares
"""
import sys
from datetime import date
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from load_snapshot import load  # noqa: E402

from etl import db, pipeline  # noqa: E402
from etl.__main__ import init_dw  # noqa: E402
from etl.config import Config  # noqa: E402

ASOF = date(2027, 1, 15)      # the notebook's as-of date


def compare_with_gold(zones, cfg):
    fails, checks = [], 0
    G = lambda n: pd.read_csv(Path(zones) / "gold" / f"{n}.csv", keep_default_na=False, na_values=[""])
    with db.connect(cfg.dw_dsn, cfg) as conn:
        q = lambda s: pd.DataFrame(conn.execute(s).fetchall())

        def compare(name, s_df, n_df, key):
            nonlocal checks
            s_, n_ = s_df.set_index(key).sort_index(), n_df.set_index(key).sort_index()
            if len(s_) != len(n_) or not s_.index.equals(n_.index):
                fails.append(f"{name}: row sets differ ({len(s_)} pipeline vs {len(n_)} notebook)")
                return
            for c in n_.columns:
                if c not in s_.columns:
                    continue
                a, b = s_[c], n_[c]
                if pd.api.types.is_numeric_dtype(b) or c.endswith(("Hours", "Rate", "Amount", "Share", "Minutes", "Days", "Price")):
                    a, b = pd.to_numeric(a, errors="coerce").astype(float), pd.to_numeric(b, errors="coerce").astype(float)
                    bad = ~((a.isna() & b.isna()) | ((a - b).abs() <= 0.0101))
                else:
                    bad = ~((a.isna() & b.isna()) | (a.astype(str) == b.astype(str)))
                checks += len(a)
                if bad.any():
                    fails.append(f"{name}.{c}: {int(bad.sum())} differences, e.g. {list(zip(a[bad][:3], b[bad][:3]))}")

        dpk, dem = G("DimPackage"), G("DimEmployee")
        pk_nb, em_nb = dict(zip(dpk.PackageKey, dpk.PackageID)), dict(zip(dem.EmployeeKey, dem.EmployeeID))
        fi_sql = q('''SELECT f.*, p."PackageID" ind_pkg, e."EmployeeID" responder FROM mart.v_inquiry f
                      JOIN dim."DimPackage" p ON p."PackageKey"=f."IndicativePackageKey"
                      JOIN dim."DimEmployee" e ON e."EmployeeKey"=f."FirstResponderEmployeeKey"''')
        fi_nb = G("FactInquiry").assign(ind_pkg=lambda x: x.IndicativePackageKey.map(pk_nb),
                                        responder=lambda x: x.FirstResponderEmployeeKey.map(em_nb))
        drop = ["InquiryFactKey", "IndicativePackageKey", "FirstResponderEmployeeKey"]
        compare("FactInquiry", fi_sql.drop(columns=drop, errors="ignore"), fi_nb.drop(columns=drop, errors="ignore"), "InquiryID")
        fe_sql = q('SELECT f.*, p."PackageID" pkg FROM fact."FactEventBooking" f JOIN dim."DimPackage" p USING ("PackageKey")')
        fe_nb = G("FactEventBooking").assign(pkg=lambda x: x.PackageKey.map(pk_nb))
        compare("FactEventBooking", fe_sql.drop(columns=["EventFactKey", "PackageKey"]), fe_nb.drop(columns=["PackageKey"]), "EventID")
        fw_sql = q('''SELECT f.*, e."EmployeeID", round(f."TotalAssignedHours" / nullif(f."StandardWeeklyHours",0), 4) AS "UtilizationRate"
                     FROM fact."FactEmployeeWeeklyWorkload" f JOIN dim."DimEmployee" e USING ("EmployeeKey")''')
        fw_nb = G("FactEmployeeWeeklyWorkload").assign(EmployeeID=lambda x: x.EmployeeKey.map(em_nb))
        compare("FactEmployeeWeeklyWorkload", fw_sql.drop(columns=["EmployeeWeekKey", "EmployeeKey"]),
                fw_nb.drop(columns=["EmployeeKey"]), ["EmployeeID", "WeekStartDateKey"])
    return checks, fails


if __name__ == "__main__":
    zones = sys.argv[1] if len(sys.argv) > 1 else "data/sim/zones_v3"
    cfg = Config()
    print("loaded into CRM:", load(zones, 1, cfg))
    init_dw(cfg, drop=True)
    pipeline.run(cfg, ASOF, "full")
    checks, fails = compare_with_gold(zones, cfg)
    print(f"values compared: {checks}; differences: {len(fails)}")
    for f in fails:
        print("  FAIL", f)
    sys.exit(1 if fails else 0)
