from __future__ import annotations

import argparse
import csv
import json
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from statistics import median
from urllib.request import Request, urlopen

OKX_TICKER = "https://www.okx.com/api/v5/market/ticker?instId={symbol}-USDT"
COINGECKO_SIMPLE = "https://api.coingecko.com/api/v3/simple/price?ids={coin_id}&vs_currencies=usd"
MEMPOOL_FEES = "https://mempool.space/api/v1/fees/recommended"
DUNE_QUERY_RESULTS = "https://api.dune.com/api/v1/query/{query_id}/results?limit={limit}"
DEFAULT_DATA_DIR = Path("data")


@dataclass
class ChainQuote:
    name: str
    fee_usdt: float
    metric: str


def _http_get_json(url: str, method: str = "GET", data: bytes | None = None, timeout: int = 12, headers: dict | None = None) -> dict:
    req_headers = {"user-agent": "gas-window-finder/0.2"}
    if data is not None:
        req_headers["content-type"] = "application/json"
    if headers:
        req_headers.update(headers)
    req = Request(url, data=data, method=method, headers=req_headers)
    with urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _rpc(url: str, rpc_method: str, params: list | None = None) -> dict:
    payload = {"jsonrpc": "2.0", "id": 1, "method": rpc_method, "params": params or []}
    return _http_get_json(url, method="POST", data=json.dumps(payload).encode("utf-8"))


def get_okx_price_usdt(symbol: str) -> float:
    obj = _http_get_json(OKX_TICKER.format(symbol=symbol))
    return float(obj["data"][0]["last"])


def get_price_usdt(symbol: str, coin_id: str) -> float:
    try:
        return get_okx_price_usdt(symbol)
    except Exception:
        obj = _http_get_json(COINGECKO_SIMPLE.format(coin_id=coin_id))
        return float(obj[coin_id]["usd"])


def estimate_evm_transfer_fee_usdt(rpc_url: str, native_price_usdt: float, gas_limit: int = 21000) -> tuple[float, float]:
    obj = _rpc(rpc_url, "eth_gasPrice")
    gas_wei = int(obj["result"], 16)
    gas_price_gwei = gas_wei / 1e9
    fee_native = (gas_wei * gas_limit) / 1e18
    fee_usdt = fee_native * native_price_usdt
    return gas_price_gwei, fee_usdt


def estimate_btc_transfer_fee_usdt(btc_price: float, tx_vbytes: int = 140, fee_mode: str = "hourFee") -> tuple[float, float]:
    obj = _http_get_json(MEMPOOL_FEES)
    sat_per_vb = float(obj.get(fee_mode, obj.get("hourFee", 1)))
    fee_btc = (sat_per_vb * tx_vbytes) / 100_000_000
    return sat_per_vb, fee_btc * btc_price


def estimate_sol_transfer_fee_usdt(sol_price: float) -> tuple[float, float]:
    obj = _rpc("https://api.mainnet-beta.solana.com", "getRecentPrioritizationFees", [])
    fees = [int(x.get("prioritizationFee", 0)) for x in obj.get("result", []) if isinstance(x, dict)]
    p50_micro_lamports_per_cu = float(median(fees)) if fees else 0.0
    base_lamports = 5000  # base fee per signature
    assumed_cu = 5000
    priority_lamports = p50_micro_lamports_per_cu * assumed_cu / 1_000_000
    fee_sol = (base_lamports + priority_lamports) / 1_000_000_000
    return p50_micro_lamports_per_cu, fee_sol * sol_price


def build_quotes(include_btc: bool = True, include_sol: bool = True) -> list[ChainQuote]:
    eth = get_price_usdt("ETH", "ethereum")
    bnb = get_price_usdt("BNB", "binancecoin")

    chains = [
        ("BSC", "https://bsc-dataseed.binance.org", bnb),
        ("Arbitrum", "https://arb1.arbitrum.io/rpc", eth),
        ("Optimism", "https://mainnet.optimism.io", eth),
        ("Base", "https://mainnet.base.org", eth),
        ("Linea", "https://rpc.linea.build", eth),
    ]

    out: list[ChainQuote] = []
    for name, rpc_url, native_price in chains:
        try:
            gwei, fee = estimate_evm_transfer_fee_usdt(rpc_url, native_price)
            out.append(ChainQuote(name=name, fee_usdt=fee, metric=f"gas={gwei:.6f} gwei"))
        except Exception:
            continue

    if include_btc:
        try:
            btc_price = get_price_usdt("BTC", "bitcoin")
            sat_vb, fee = estimate_btc_transfer_fee_usdt(btc_price)
            out.append(ChainQuote(name="Bitcoin", fee_usdt=fee, metric=f"fee={sat_vb:.0f} sat/vB"))
        except Exception:
            pass

    if include_sol:
        try:
            sol_price = get_price_usdt("SOL", "solana")
            p50, fee = estimate_sol_transfer_fee_usdt(sol_price)
            out.append(ChainQuote(name="Solana", fee_usdt=fee, metric=f"priority_p50={p50:.0f} μ-lamports/CU"))
        except Exception:
            pass

    out.sort(key=lambda x: x.fee_usdt, reverse=True)
    return out


def format_quotes(quotes: list[ChainQuote]) -> str:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [f"[{now}] 常用链转账费估算（USDT，高→低）"]
    for idx, q in enumerate(quotes, start=1):
        lines.append(f"{idx}. {q.name:<9} fee={q.fee_usdt:.6f} USDT   {q.metric}")
    return "\n".join(lines)


def collect_evm_snapshot(data_dir: Path = DEFAULT_DATA_DIR) -> Path:
    eth = get_price_usdt("ETH", "ethereum")
    bnb = get_price_usdt("BNB", "binancecoin")
    chains = [
        ("BSC", "https://bsc-dataseed.binance.org", bnb),
        ("Arbitrum", "https://arb1.arbitrum.io/rpc", eth),
        ("Optimism", "https://mainnet.optimism.io", eth),
        ("Base", "https://mainnet.base.org", eth),
        ("Linea", "https://rpc.linea.build", eth),
        ("Ethereum", "https://ethereum.publicnode.com", eth),
    ]

    data_dir.mkdir(parents=True, exist_ok=True)
    out_path = data_dir / "gas_snapshots.csv"
    existed = out_path.exists()

    with out_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not existed:
            writer.writerow(["chain", "timestamp_iso", "fee_native"])
        ts = datetime.now(UTC).isoformat()
        for name, rpc_url, _ in chains:
            try:
                gwei, _ = estimate_evm_transfer_fee_usdt(rpc_url, 1.0)
                writer.writerow([name, ts, f"{gwei:.9f}"])
            except Exception:
                continue

    return out_path


def _parse_iso(ts: str) -> datetime:
    t = datetime.fromisoformat(ts)
    if t.tzinfo is None:
        return t.replace(tzinfo=UTC)
    return t.astimezone(UTC)


def low_windows_from_snapshots(rows: list[dict], now_utc: datetime | None = None, top_n: int = 3) -> dict[str, list[tuple[int, float]]]:
    if now_utc is None:
        now_utc = datetime.now(UTC)
    start = now_utc - timedelta(hours=24)

    by_chain_hour: dict[str, dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
    for row in rows:
        ts = _parse_iso(row["timestamp_iso"])
        if ts < start or ts > now_utc:
            continue
        # force Asia/Shanghai representation by +8 from UTC
        cst_hour = (ts.hour + 8) % 24
        by_chain_hour[row["chain"]][cst_hour].append(float(row["fee_native"]))

    out: dict[str, list[tuple[int, float]]] = {}
    for chain, hour_map in by_chain_hour.items():
        ranked = sorted(
            ((hour, sum(vals) / len(vals)) for hour, vals in hour_map.items()),
            key=lambda x: x[1],
        )
        out[chain] = ranked[:top_n]
    return out


def format_windows(windows: dict[str, list[tuple[int, float]]]) -> str:
    if not windows:
        return "过去24小时样本不足，暂无低费窗口。"
    lines = ["过去24小时低 gas 窗口（CST，按小时）"]
    for chain, items in sorted(windows.items()):
        lines.append(f"- {chain}:")
        for i, (hour, avg_fee) in enumerate(items, start=1):
            next_h = (hour + 1) % 24
            lines.append(f"  P{i} {hour:02d}:00-{next_h:02d}:00  avg={avg_fee:.6f} gwei")
    return "\n".join(lines)


def load_snapshots(csv_path: Path) -> list[dict]:
    if not csv_path.exists():
        return []
    with csv_path.open("r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def daily_weekly_from_dune_rows(rows: list[dict], chains: list[str]) -> dict[str, tuple[float, float]]:
    out: dict[str, tuple[float, float]] = {}
    for chain in chains:
        vals = [float(r[chain]) for r in rows[:7] if chain in r and isinstance(r[chain], (int, float))]
        if vals:
            out[chain] = (vals[0], sum(vals) / len(vals))
    return out


def fetch_dune_daily_weekly(query_id: int = 4096073, limit: int = 14) -> list[dict]:
    api_key = os.getenv("DUNE_API_KEY", "")
    if not api_key:
        raise RuntimeError("DUNE_API_KEY 未设置")
    url = DUNE_QUERY_RESULTS.format(query_id=query_id, limit=limit)
    obj = _http_get_json(url, headers={"X-Dune-API-Key": api_key})
    return obj.get("result", {}).get("rows", [])


def format_daily_weekly(stats: dict[str, tuple[float, float]]) -> str:
    if not stats:
        return "未获取到日均/周均统计。"
    lines = ["单日/周均 gas fee（Dune daily 口径）"]
    for chain, (day, week) in stats.items():
        lines.append(f"- {chain}: latest_day={day:.2f}, week_avg={week:.2f}")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(prog="gas-window-finder")
    sub = parser.add_subparsers(dest="command", required=True)

    p_quote = sub.add_parser("quote", help="Get realtime transfer fee ranking in USDT")
    p_quote.add_argument("--no-btc", action="store_true")
    p_quote.add_argument("--no-sol", action="store_true")

    p_collect = sub.add_parser("collect", help="Collect current EVM gas snapshots")
    p_collect.add_argument("--data-dir", default="data")

    p_win = sub.add_parser("windows24", help="Compute low gas windows from last 24h snapshots")
    p_win.add_argument("--csv", default="data/gas_snapshots.csv")

    p_stats = sub.add_parser("stats", help="Fetch daily/weekly stats from Dune")
    p_stats.add_argument("--query-id", type=int, default=4096073)

    args = parser.parse_args()

    if args.command == "quote":
        quotes = build_quotes(include_btc=not args.no_btc, include_sol=not args.no_sol)
        if not quotes:
            print("未获取到可用链上报价，请稍后重试。")
            return
        print(format_quotes(quotes))
        return

    if args.command == "collect":
        out = collect_evm_snapshot(Path(args.data_dir))
        print(f"ok: snapshot written to {out}")
        return

    if args.command == "windows24":
        rows = load_snapshots(Path(args.csv))
        windows = low_windows_from_snapshots(rows)
        print(format_windows(windows))
        return

    if args.command == "stats":
        rows = fetch_dune_daily_weekly(query_id=args.query_id)
        stats = daily_weekly_from_dune_rows(rows, ["bitcoin", "solana", "ethereum", "arbitrum", "base", "bnb"])
        print(format_daily_weekly(stats))
        return


if __name__ == "__main__":
    main()
