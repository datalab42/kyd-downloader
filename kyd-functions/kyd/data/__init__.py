
from textparser import PortugueseRulesParser, GenericParser


class PortugueseRulesParser2(PortugueseRulesParser):
    def parseDate_ptBR(self, text, match):
        r'(\d{2})/(\d{2})/(\d{4})'
        return '{}-{}-{}'.format(match.group(3), match.group(2), match.group(1))


def convert_csv_to_dict(file, sep=';', encoding='utf-8'):
    parser = GenericParser()
    for ix, line in enumerate(file):
        line = line.decode(encoding).strip()
        if ix == 0:
            hdr = [field.lower() for field in line.split(sep)]
        vals = [parser.parse(val) for val in line.split(sep)]
        yield dict(zip(hdr, vals))


