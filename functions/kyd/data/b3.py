
import json
import zipfile
import logging
from itertools import groupby

from lxml import etree

from .read_fwf import read_fwf

from . import PortugueseRulesParser2

class TaxaSwap:
    widths = [6, 3, 2, 8, 2, 5, 15, 5, 5, 1, 14, 1, 5]
    colnames = [
        'id_transacao',
        'compl_transacao',
        'tipo_registro',
        'data_geracao_arquivo',
        'cod_curvas',
        'cod_taxa',
        'desc_taxa',
        'num_dias_corridos',
        'num_dias_saques',
        'sinal_taxa',
        'taxa_teorica',
        'carat_vertice',
        'cod_vertice'
    ]

    def __init__(self, rawdata):
        self.__data = read_fwf(rawdata, self.widths, self.colnames, parse_fun=self._parse)
        self.__findata = [self._build_findata(list(v)) for k, v in groupby(self.__data, key=lambda x: x['cod_taxa'])]
    
    def _parse(self, obj):
        obj['data_geracao_arquivo'] = '{}-{}-{}'.format(obj['data_geracao_arquivo'][:4], obj['data_geracao_arquivo'][4:6], obj['data_geracao_arquivo'][6:])
        obj['num_dias_corridos'] = int(obj['num_dias_corridos'])
        obj['num_dias_saques'] = int(obj['num_dias_saques'])
        obj['sinal_taxa'] = 1 if obj['sinal_taxa'] == '+' else -1
        obj['taxa_teorica'] = float(obj['taxa_teorica'])/1e7
        return obj

    def _build_findata(self, lst):
        taxa_teorica = [obj['taxa_teorica']*obj['sinal_taxa'] for obj in lst]
        num_dias_corridos = [obj['num_dias_corridos'] for obj in lst]
        num_dias_saques = [obj['num_dias_saques'] for obj in lst]
        carat_vertice = [obj['carat_vertice'] for obj in lst]
        keys = ('current_days', 'business_days', 'type', 'value')
        terms = [dict(zip(keys, x)) for x in zip(num_dias_corridos, num_dias_saques, carat_vertice, taxa_teorica)]

        return {
            'refdate': lst[0]['data_geracao_arquivo'],
            'id': lst[0]['cod_taxa'],
            'name': lst[0]['cod_curvas'],
            'description': lst[0]['desc_taxa'],
            'terms': terms
        }

    @property
    def data(self):
        return self.__data

    @property
    def findata(self):
        return self.__findata


def download_unzip_and_get_content(temp, encode=True, encoding='latin1'):
    zf = zipfile.ZipFile(temp)
    name = zf.namelist()[-1]
    logging.info('zipped file %s', name)
    content = zf.read(name)
    zf.close()
    temp.close()
    if encode:
        return content.decode(encoding)
    else:
        return content


def parse_taxa_cdi_idi(text):
    text_parser = PortugueseRulesParser2()
    cdi_data = json.loads(text)
    cdi_prices_data = {
        'trade_date': text_parser.parse(cdi_data['dataTaxa']),
        'last_price': text_parser.parse(cdi_data['taxa']),
        'ticker_symbol': 'CDI'
    }
    idi_prices_data = {
        'trade_date': text_parser.parse(cdi_data['dataIndice']),
        'last_price': text_parser.parse(cdi_data['indice']),
        'ticker_symbol': 'IDI'
    }
    return cdi_prices_data, idi_prices_data

