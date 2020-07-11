
from datetime import datetime
from google.cloud import datastore

def save_download_logs(data):
    print(data)
    client = datastore.Client()
    key = client.key('DownloadLog')
    log_entry = datastore.Entity(key)
    data['refdate'] = data['refdate'] and datetime.strptime(data['refdate'], '%Y-%m-%d')
    data['time'] = datetime.strptime(data['time'], '%Y-%m-%dT%H:%M:%S.%f')
    log_entry.update(data)
    client.put(log_entry)
