
import csv
from datetime import datetime
from itertools import dropwhile
from io import StringIO

import lxml.html

from . import PortugueseRulesParser2


def parse_titpub(text, buf):
    pp = PortugueseRulesParser2()
    text = StringIO(text.decode('ISO-8859-1'))
    writer = csv.writer(buf)
    _drop_first_2 = dropwhile(lambda x: x[0] < 2, enumerate(text))
    _drop_empy = filter(lambda x: x[1].strip() is not '', _drop_first_2)
    for c, line in _drop_empy:
        row = line.split('@')
        tit = dict(
            titulo=row[0],
            data_referencia=row[1],
            codigo_selic=row[2],
            data_base=row[3],
            data_vencimento=row[4],
            taxa_max=pp.parse(row[5]),
            taxa_min=pp.parse(row[6]),
            taxa_ind=pp.parse(row[7]),
            pu=pp.parse(row[8]),
            desvio_padrao=pp.parse(row[9])
        )
        writer.writerow(tit.values())
    date_str = datetime.strptime(tit['data_referencia'], '%Y%m%d').strftime('%Y-%m-%d')
    return date_str


def parse_vnataxatitpub(text, buf):
    pp = PortugueseRulesParser2()
    writer = csv.writer(buf)
    writer.writerow(['data_ref', 'taxa', 'valor', 'projecao', 'vigencia'])

    root = lxml.html.fromstring(text.decode('ISO-8859-1'))
    
    sel = "div#listaNTN-B > center > table > tr:nth-child(2) > td:nth-child(2)"
    date = root.cssselect(sel)[0].text_content()
    sel = "div#listaNTN-B > center > table > tr:nth-child(4) > td:nth-child(3)"
    value = root.cssselect(sel)[0].text_content()
    sel = "div#listaNTN-B > center > table > tr:nth-child(4) > td:nth-child(4)"
    projecao = root.cssselect(sel)[0].text_content()
    sel = "div#listaNTN-B > center > table > tr:nth-child(4) > td:nth-child(5)"
    vigencia = root.cssselect(sel)[0].text_content()
    writer.writerow([pp.parse(date), 'IPCA', pp.parse(value), projecao, pp.parse(vigencia)])
    
    sel = "div#listaNTN-C > center > table > tr:nth-child(2) > td:nth-child(2)"
    date = root.cssselect(sel)[0].text_content()
    sel = "div#listaNTN-C > center > table > tr:nth-child(4) > td:nth-child(3)"
    value = root.cssselect(sel)[0].text_content()
    sel = "div#listaNTN-C > center > table > tr:nth-child(4) > td:nth-child(4)"
    projecao = root.cssselect(sel)[0].text_content()
    sel = "div#listaNTN-C > center > table > tr:nth-child(4) > td:nth-child(5)"
    vigencia = root.cssselect(sel)[0].text_content()
    writer.writerow([pp.parse(date), 'IGPM', pp.parse(value), projecao, pp.parse(vigencia)])
    
    sel = "div#listaLFT > center > table > tr:nth-child(2) > td:nth-child(2)"
    date = root.cssselect(sel)[0].text_content()
    sel = "div#listaLFT > center > table > tr:nth-child(4) > td:nth-child(3)"
    value = root.cssselect(sel)[0].text_content()
    writer.writerow([pp.parse(date), 'SELIC', pp.parse(value), None, None])

    return pp.parse(date)


def parse_vnatitpub(text, buf):
    pp = PortugueseRulesParser2()
    writer = csv.writer(buf)
    writer.writerow(['data_ref', 'titulo', 'VNA'])

    root = lxml.html.fromstring(text.decode('ISO-8859-1'))
    
    sel = "div#listaNTN-B > center > table > tr:nth-child(2) > td:nth-child(2)"
    date = root.cssselect(sel)[0].text_content()
    sel = "div#listaNTN-B > center > table > tr:nth-child(4) > td:nth-child(2)"
    value = root.cssselect(sel)[0].text_content()
    writer.writerow([pp.parse(date), 'NTNB', pp.parse(value)])
    
    sel = "div#listaNTN-C > center > table > tr:nth-child(2) > td:nth-child(2)"
    date = root.cssselect(sel)[0].text_content()
    sel = "div#listaNTN-C > center > table > tr:nth-child(4) > td:nth-child(2)"
    value = root.cssselect(sel)[0].text_content()
    writer.writerow([pp.parse(date), 'NTNC', pp.parse(value)])
    
    sel = "div#listaLFT > center > table > tr:nth-child(2) > td:nth-child(2)"
    date = root.cssselect(sel)[0].text_content()
    sel = "div#listaLFT > center > table > tr:nth-child(4) > td:nth-child(2)"
    value = root.cssselect(sel)[0].text_content()
    writer.writerow([pp.parse(date), 'LFT', pp.parse(value)])

    return pp.parse(date)

