from datetime import datetime, timedelta, timezone

import pytz
from flask import Flask
from flask import render_template
from flask import request, url_for
from flask_table import Table, Col

from google.cloud import datastore


# If `entrypoint` is not defined in app.yaml, App Engine will look for an app
# called `app` in `main.py`.
app = Flask(__name__)
client = datastore.Client()
SP_TZ = pytz.timezone('America/Sao_Paulo')
UTC = pytz.utc

class DownloadLogTable(Table):
    name = Col('Name')
    time = Col('Time')
    refdate = Col('Refdate')
    download_status = Col('DownloadStatus')
    status = Col('Status')
    filename = Col('Filename')
    message = Col('Message')
    allow_sort = True
    classes = ['table', 'table-striped', 'table-sm']

    def sort_url(self, col_key, reverse=False):
        if reverse:
            direction =  'desc'
        else:
            direction = 'asc'
        return url_for('index', sort=col_key, direction=direction)


@app.route('/')
def index():
    date = datetime.now(SP_TZ) + timedelta(-1)
    if request.args.get('date'):
        date = request.args.get('date')
        date = datetime.strptime(date, '%Y-%m-%d')
        date = SP_TZ.localize(date)

    t1 = date.replace(hour=0, minute=0, second=0, microsecond=0)
    t2 = date.replace(hour=23, minute=59, second=59, microsecond=999999)
    t1 = t1.astimezone(UTC)
    t2 = t2.astimezone(UTC)

    q = client.query(kind='DownloadLog')
    q.add_filter('time', '>=', t1)
    q.add_filter('time', '<=', t2)
    logs = list(q.fetch())
    for log in logs:
        if log['refdate']:
            log['refdate'] = log['refdate'].astimezone(SP_TZ).strftime('%Y-%m-%d')
        else:
            log['refdate'] = ''
        log['time'] = log['time'].astimezone(SP_TZ).strftime('%Y-%m-%d %H:%M:%S.%f')
        if log['status'] == 0:
            log['filename'] = 'gs://{}/{}'.format(log['bucket'], log['filename'])

    sort = request.args.get('sort', 'name')
    reverse = (request.args.get('direction', 'asc') == 'desc')
    logs_sorted = logs.copy()
    logs_sorted.sort(key=lambda x: x[sort], reverse=reverse)
    tb = DownloadLogTable(logs_sorted, sort_by=sort, sort_reverse=reverse)

    return render_template('base.html', date=date, length=len(logs_sorted), table=tb)


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)