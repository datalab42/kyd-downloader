
from datetime import datetime, timedelta, timezone

from google.cloud import datastore

client = datastore.Client()

q = client.query(kind='DownloadLog')
t = datetime.utcnow() + timedelta(-2)
t = t.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
print(t)
q.add_filter('time', '>=', t)
q.add_filter('time', '<=', t+timedelta(1))
for ix, d in enumerate(q.fetch()):
    print(ix, d['name'], d['time'])