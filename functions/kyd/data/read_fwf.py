

def read_fwf(con, widths, colnames=None, skip=0, parse_fun=lambda x: x):
    colpositions = []
    x = 0
    for w in widths:
        colpositions.append((x, x+w))
        x = x + w

    colnames = ['V{}'.format(ix+1) for ix in range(len(widths))] if colnames is None else colnames

    terms = []
    for ix, line in enumerate(con):
        if ix < skip:
            continue
        line = line.strip()
        fields = [line[dx[0]:dx[1]].strip() for dx in colpositions]
        obj = dict((k, v) for k, v in zip(colnames, fields))
        terms.append(parse_fun(obj))
    
    return terms

