
from datetime import datetime, timezone

import pytz

from google.cloud import datastore

def save_download_logs(data):
    client = datastore.Client()
    key = client.key('DownloadLog')
    log_entry = datastore.Entity(key)
    data['refdate'] = data['refdate'] and datetime.strptime(data['refdate'], '%Y-%m-%dT%H:%M:%S.%f%z')
    if data['refdate']:
        data['refdate'].replace(tzinfo=pytz.timezone('America/Sao_Paulo'))
    data['time'] = datetime.strptime(data['time'], '%Y-%m-%dT%H:%M:%S.%f%z')
    # data['time'].replace(tzinfo=timezone.utc)
    log_entry.update(data)
    client.put(log_entry)
