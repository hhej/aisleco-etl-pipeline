"""Source-to-target on realistic volume: the notebook's simulated CRM goes through the
pipeline and the three facts are compared value by value with the notebook's Gold zone,
which pandas computed independently. Needs data/sim/zones_v3 (see README)."""
from datetime import date
from pathlib import Path

import pytest

from crosscheck import compare_with_gold
from etl import pipeline
from etl.__main__ import init_dw
from load_snapshot import load

ZONES = Path(__file__).resolve().parent.parent / "data" / "sim" / "zones_v3"


@pytest.mark.skipif(not (ZONES / "gold").exists(), reason="run the v3 notebook first (README, step 3)")
def test_pipeline_matches_notebook_gold(cfg):
    """Scenario: The simulated business year (619 inquiries, 2,652 messages, 345 assignments) through the pipeline, vs the facts pandas computed independently (v3 notebook).
    Expected: 28,638 values compared, 0 differences."""
    load(ZONES, 1, cfg)
    init_dw(cfg, drop=True)
    pipeline.run(cfg, date(2027, 1, 15), "full")
    checks, fails = compare_with_gold(ZONES, cfg)
    assert checks == 28638      # 29257 before InquiryStatus left the warehouse (619 inquiries)
    assert fails == []
