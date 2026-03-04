from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime
from urllib.request import Request, urlopen

OKX_TICKER = "https://www.okx.com/api/v5/market/ticker?instId={symbol}-USDT"
COINGECKO_SIMPLE = "https://api.coingecko.com/api/v3/simple/price?ids={coin_id}&vs_currencies=usd"


@dataclass
class ChainQuote:
    name: str
    gas_price_gwei: float
    fee_usdt: float


def _http_get_json(url: str, method: str = "GET", data: bytes | None = None, timeout: int = 12) -> dict:
    headers = {"user-agent": "gas-window-finder/0.1"}
    if data is not None:
        headers["content-type"] = "application/json"
    req = Request(url, data=data, method=method, headers=headers)
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


def build_quotes() -> list[ChainQuote]:
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
            out.append(ChainQuote(name=name, gas_price_gwei=gwei, fee_usdt=fee))
        except Exception:
            continue

    out.sort(key=lambda x: x.fee_usdt, reverse=True)
    return out


def format_quotes(quotes: list[ChainQuote]) -> str:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [f"[{now}] EVM L2/L1 转账费估算（USDT，高→低）"]
    for idx, q in enumerate(quotes, start=1):
        lines.append(f"{idx}. {q.name:<9} fee={q.fee_usdt:.6f} USDT   gas={q.gas_price_gwei:.6f} gwei")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(prog="gas-window-finder")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("quote", help="Get realtime EVM transfer fee ranking in USDT")
    args = parser.parse_args()

    if args.command == "quote":
        quotes = build_quotes()
        if not quotes:
            print("未获取到可用链上报价，请稍后重试。")
            return
        print(format_quotes(quotes))


if __name__ == "__main__":
    main()
