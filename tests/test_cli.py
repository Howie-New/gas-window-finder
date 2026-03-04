from datetime import UTC, datetime

from gas_window_finder.cli import ChainQuote, daily_weekly_from_dune_rows, format_quotes, low_windows_from_snapshots


def test_format_quotes_contains_header_and_items():
    quotes = [
        ChainQuote(name="Linea", fee_usdt=0.0018, metric="gas=0.04 gwei"),
        ChainQuote(name="Arbitrum", fee_usdt=0.0009, metric="gas=0.02 gwei"),
    ]
    text = format_quotes(quotes)
    assert "常用链转账费估算" in text
    assert "1. Linea" in text
    assert "2. Arbitrum" in text


def test_low_windows_from_snapshots_selects_lowest_hours():
    now = datetime(2026, 3, 5, 0, 0, tzinfo=UTC)
    rows = [
        {"chain": "BSC", "timestamp_iso": "2026-03-04T00:10:00+00:00", "fee_native": "0.050"},  # 08 CST
        {"chain": "BSC", "timestamp_iso": "2026-03-04T01:10:00+00:00", "fee_native": "0.020"},  # 09 CST
        {"chain": "BSC", "timestamp_iso": "2026-03-04T02:10:00+00:00", "fee_native": "0.030"},  # 10 CST
    ]
    windows = low_windows_from_snapshots(rows, now_utc=now, top_n=2)
    assert "BSC" in windows
    assert windows["BSC"][0][0] == 9
    assert round(windows["BSC"][0][1], 3) == 0.020


def test_daily_weekly_from_dune_rows():
    rows = [
        {"ethereum": 10.0, "bitcoin": 4.0},
        {"ethereum": 20.0, "bitcoin": 6.0},
    ]
    stats = daily_weekly_from_dune_rows(rows, ["ethereum", "bitcoin"])
    assert stats["ethereum"][0] == 10.0
    assert stats["ethereum"][1] == 15.0
    assert stats["bitcoin"][1] == 5.0
