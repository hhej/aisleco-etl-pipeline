"""Tests run against the local PostgreSQL, in their own databases (aisleco_*_test), so the
demo databases are never touched."""
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

os.environ["ETL_CRM_DSN"] = "dbname=aisleco_crm_test"
os.environ["ETL_DW_DSN"] = "dbname=aisleco_dw_test"
os.environ["ETL_RETRY_BASE_SECONDS"] = "0.05"

from etl import db  # noqa: E402
from etl.config import Config  # noqa: E402


@pytest.fixture
def cfg(tmp_path, monkeypatch):
    monkeypatch.setenv("ETL_LANDING_DIR", str(tmp_path / "landing"))
    return Config()


def q(cfg, sql, params=None, dsn=None):
    with db.connect(dsn or cfg.dw_dsn, cfg) as c:
        return c.execute(sql, params).fetchall()


def facts(cfg, dsn=None):
    """The three facts with surrogate keys replaced by business keys, for comparisons."""
    import pandas as pd
    out = {}
    # surrogate keys depend on load order, so compare business keys instead
    for name, sql, key in [
        ("FactInquiry", 'SELECT f.*, p."PackageID" AS ind_pkg, e."EmployeeID" AS responder FROM mart.v_inquiry f '
                        'JOIN dim."DimPackage" p ON p."PackageKey"=f."IndicativePackageKey" '
                        'JOIN dim."DimEmployee" e ON e."EmployeeKey"=f."FirstResponderEmployeeKey"', ["InquiryID"]),
        ("FactEventBooking", 'SELECT f.*, p."PackageID" AS pkg FROM fact."FactEventBooking" f '
                             'JOIN dim."DimPackage" p USING ("PackageKey")', ["EventID"]),
        ("FactEmployeeWeeklyWorkload", 'SELECT w.*, e."EmployeeID" FROM fact."FactEmployeeWeeklyWorkload" w '
                                       'JOIN dim."DimEmployee" e USING ("EmployeeKey")', ["EmployeeID", "WeekStartDateKey"])]:
        df = pd.DataFrame(q(cfg, sql, dsn=dsn))
        df = df.drop(columns=[c for c in df.columns if c in ("InquiryFactKey", "EventFactKey", "EmployeeWeekKey", "EmployeeKey", "OutcomeKey",
                                                               "IndicativePackageKey", "FirstResponderEmployeeKey", "PackageKey")])
        out[name] = df.set_index(key).sort_index()
    return out
