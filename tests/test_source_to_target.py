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
    """Scenario: The simulated three business years (1,905 inquiries, 22,432 messages, 1,040 assignments) through the pipeline, vs the facts pandas computed independently (v3 notebook).
    Expected: 76,770 values compared, 0 differences."""
    load(ZONES, 1, cfg)
    init_dw(cfg, drop=True)
    pipeline.run(cfg, date(2027, 1, 15), "full")
    checks, fails = compare_with_gold(ZONES, cfg)
    assert checks == 76770      # three simulated years (1,905 inquiries)
    assert fails == []
