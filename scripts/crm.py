"""Source-system helpers (not part of the ETL): create the CRM database (OLTP v5) and
run SQL against it. Used by the walkthrough, the snapshot loader, the tests and the demo."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import psycopg  # noqa: E402
from psycopg import sql  # noqa: E402

from etl import db  # noqa: E402
from etl.config import SQL_DIR, Config  # noqa: E402


def create_crm(cfg: Config = None, drop: bool = True):
    cfg = cfg or Config()
    name = psycopg.conninfo.conninfo_to_dict(cfg.crm_dsn)["dbname"]
    with db.connect(cfg.admin_dsn, cfg, autocommit=True) as c:
        if drop:
            c.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name)))
        c.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    with db.connect(cfg.crm_dsn, cfg) as c:
        db.run_sql_file(c, SQL_DIR / "WeddingOrganizer_OLTP_v5_dbdiagram.sql")
        db.run_sql_file(c, SQL_DIR / "oltp_v5_cdc_support.sql")


def crm_exec(text: str, cfg: Config = None):
    """Run SQL in the CRM as one committed transaction (psql meta-commands are skipped)."""
    cfg = cfg or Config()
    text = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("\\"))
    with db.connect(cfg.crm_dsn, cfg) as c:
        c.execute(text)


if __name__ == "__main__":
    create_crm()
    print("CRM database created (OLTP v5)")
