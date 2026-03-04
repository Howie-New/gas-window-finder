from gas_window_finder.cli import ChainQuote, format_quotes


def test_format_quotes_contains_header_and_items():
    quotes = [
        ChainQuote(name="Linea", gas_price_gwei=0.04, fee_usdt=0.0018),
        ChainQuote(name="Arbitrum", gas_price_gwei=0.02, fee_usdt=0.0009),
    ]
    text = format_quotes(quotes)
    assert "转账费估算" in text
    assert "1. Linea" in text
    assert "2. Arbitrum" in text
