
from lxml import etree

def parse_bvbg086_price_report(text):
    ns = {None: 'urn:bvmf.217.01.xsd'}
    root = etree.fromstring(text)
    attrs = {
        'trade_date': 'TradDt/Dt',
        'ticker_symbol': 'SctyId/TckrSymb',
        'security_id': 'FinInstrmId/OthrId/Id',  # SecurityId
        'trade_quantity': 'TradDtls/TradQty',  # Negócios
        'volume': 'FinInstrmAttrbts/NtlFinVol',
        'open_interest': 'FinInstrmAttrbts/OpnIntrst',
        'traded_contracts': 'FinInstrmAttrbts/FinInstrmQty',
        'best_ask_price': 'FinInstrmAttrbts/BestAskPric',
        'best_bid_price': 'FinInstrmAttrbts/BestBidPric',
        'first_price': 'FinInstrmAttrbts/FrstPric',
        'min_price': 'FinInstrmAttrbts/MinPric',
        'max_price': 'FinInstrmAttrbts/MaxPric',
        'average_price': 'FinInstrmAttrbts/TradAvrgPric',
        'last_price': 'FinInstrmAttrbts/LastPric',
        # Negócios na sessão regular
        'regular_transactions_quantity': 'FinInstrmAttrbts/RglrTxsQty',
        # Contratos na sessão regular
        'regular_traded_contracts': 'FinInstrmAttrbts/RglrTraddCtrcts',
        # Volume financeiro na sessão regular
        'regular_volume': 'FinInstrmAttrbts/NtlRglrVol',
        # Negócios na sessão não regular
        'nonregular_transactions_quantity': 'FinInstrmAttrbts/NonRglrTxsQty',
        # Contratos na sessão não regular
        'nonregular_traded_contracts': 'FinInstrmAttrbts/NonRglrTraddCtrcts',
        # Volume financeiro na sessão nãoregular
        'nonregular_volume': 'FinInstrmAttrbts/NtlNonRglrVol',
        'oscillation_percentage': 'FinInstrmAttrbts/OscnPctg',
        'adjusted_quote': 'FinInstrmAttrbts/AdjstdQt',
        'adjusted_tax': 'FinInstrmAttrbts/AdjstdQtTax',
        'previous_adjusted_quote': 'FinInstrmAttrbts/PrvsAdjstdQt',
        'previous_adjusted_tax': 'FinInstrmAttrbts/PrvsAdjstdQtTax',
        'variation_points': 'FinInstrmAttrbts/VartnPts',
        'adjusted_value_contract': 'FinInstrmAttrbts/AdjstdValCtrct',
    }
    data = {}
    for attr in attrs:
        els = root.findall(attrs[attr], ns)
        if len(els):
            data[attr] = els[0].text

    return data


def get_bvbg086_price_reports(text):
    root = etree.fromstring(text)
    exchange = root[0][0]
    xs = exchange.findall(
        '{urn:bvmf.052.01.xsd}BizGrp/{urn:bvmf.217.01.xsd}Document/{urn:bvmf.217.01.xsd}PricRpt')
    for xx in xs:
        yield etree.tostring(xx)

