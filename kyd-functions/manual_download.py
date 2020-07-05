import json
import logging
from datetime import datetime, date, timedelta

from google.cloud import storage
import bizdays

from kyd.data.downloaders import download_by_config
from main import save_file_to_output_bucket

logging.basicConfig(level=logging.DEBUG)

cal = bizdays.Calendar(weekdays=('sat', 'sun'))

with open('../config/indexreport.json') as fp:
    content = fp.read()
    dt = date(2020, 6, 26)
    while dt <= date.today():
        if not cal.isbizday(dt):
            logging.debug('Skiping non bizday %s', dt.isoformat())
        else:
            download_by_config(content, save_file_to_output_bucket, dt)
        dt += timedelta(1)
