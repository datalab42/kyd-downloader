import json
import logging
from datetime import datetime, date, timedelta

from google.cloud import storage
# import bizdays

from kyd.data.downloaders import download_by_config
from main import save_file_to_output_bucket

logging.basicConfig(level=logging.DEBUG)

# cal = bizdays.Calendar(weekdays=('sat', 'sun'))

dt = date(2020, 7, 8)
with open('../config/pricereport.json') as fp:
    content = fp.read()
    download_by_config(content, save_file_to_output_bucket, dt)
with open('../config/cadinstrindic.json') as fp:
    content = fp.read()
    download_by_config(content, save_file_to_output_bucket, dt)
with open('../config/cadinstr.json') as fp:
    content = fp.read()
    download_by_config(content, save_file_to_output_bucket, dt)
dt = date(2020, 7, 7)
with open('../config/fpr.json') as fp:
    content = fp.read()
    download_by_config(content, save_file_to_output_bucket, dt)
with open('../config/riskformulas.json') as fp:
    content = fp.read()
    download_by_config(content, save_file_to_output_bucket, dt)

# with open('../config/indexreport.json') as fp:
#     content = fp.read()
#     dt = date(2020, 7, 6)
#     download_by_config(content, save_file_to_output_bucket, dt)
