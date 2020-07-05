
import sys
import tempfile
sys.path.append('../kyd-functions/')

from kyd.data.anbima import parse_vnatitpub

import logging

logging.basicConfig(level=logging.INFO)

with open('../../2020-05-25.html', 'rb') as fp:
    content = fp.read()
    temp = tempfile.TemporaryFile('w+')
    parse_vnatitpub(content, temp)
    temp.seek(0)
    print(temp.read())