"""Command line (run after the ETL; Airflow calls the same commands):

  python -m ml features --asof D     store the features of the open inquiries (offline store)
  python -m ml score --asof D        score them with the production model
  python -m ml monitor --asof D      drift and live-accuracy checks
  python -m ml daily --asof D        the three above
  python -m ml train --asof D        train, validate, register (promote if it passes)
  python -m ml status                model versions
"""
import argparse
import logging
from datetime import date

from etl import db
from etl.config import Config

from . import features, monitor, score, train


def main(argv=None):
    p = argparse.ArgumentParser(prog="python -m ml")
    p.add_argument("cmd", choices=["features", "score", "monitor", "daily", "train", "status"])
    p.add_argument("--asof", type=date.fromisoformat, default=date.today())
    a = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S")
    cfg = Config()
    with db.connect(cfg.dw_dsn, cfg, purpose="warehouse") as c:
        if a.cmd in ("features", "daily"):
            print(f"features stored for {features.materialise(c, a.asof)} open inquiries")
        if a.cmd in ("score", "daily"):
            print(f"scored {score.score(c, a.asof)} open inquiries")
        if a.cmd in ("monitor", "daily"):
            for name, v, st, detail in monitor.monitor(c, a.asof):
                print(f"{st:<8} {name:<26} {v if v is not None else '-':>8}  {detail}")
        if a.cmd == "train":
            r = train.train(c, a.asof)
            print(f"model v{r['version']} ({r['algorithm']}): {r['status']} - {r['decision']}")
        if a.cmd == "status":
            for r in c.execute('SELECT "Version","AsOfDate","Algorithm","Status","Metrics"->\'chosen\'->>\'auc\' AS auc,"Decision" '
                               'FROM ml."ModelVersion" ORDER BY 1').fetchall():
                print(f'v{r["Version"]:<3} data to {r["AsOfDate"]}  {r["Algorithm"]:<20} {r["Status"]:<10} AUC {r["auc"]}  {r["Decision"]}')


if __name__ == "__main__":
    main()
