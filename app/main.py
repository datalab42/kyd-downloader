
import logging
from datetime import datetime, timedelta, timezone

import pytz
from flask import Flask
from flask import render_template
from flask import request, url_for
from flask_table import Table, Col
from flask_table.html import element
import requests

from google.cloud import datastore

logging.basicConfig(level=logging.INFO)


# If `entrypoint` is not defined in app.yaml, App Engine will look for an app
# called `app` in `main.py`.
app = Flask(__name__)
client = datastore.Client()
SP_TZ = pytz.timezone('America/Sao_Paulo')
UTC = pytz.utc


class URLCol(Col):
    def __init__(self, name, **kwargs):
        super(URLCol, self).__init__(name, **kwargs)

    def td_contents(self, item, attr_list):
        name = self.from_attr_list(item, 'name')
        refdate = self.from_attr_list(item, 'refdate')
        # url = '/reprocess?filter={}&refdate={}'.format(name, refdate)
        url = 'https://us-central1-kyd-storage-001.cloudfunctions.net/kyd-generic-download-publish?filter={}&refdate={}'.format(name, refdate)
        return element('a', {'href': url}, content='Link')

# -H "Authorization: bearer $(gcloud auth print-identity-token)"

class DownloadLogTable(Table):
    name = Col('Name')
    time = Col('Time')
    refdate = Col('Refdate')
    download_status = Col('DownloadStatus')
    status = Col('Status')
    filename = Col('Filename')
    message = Col('Message')
    url = URLCol('URL')
    allow_sort = True
    classes = ['table', 'table-striped', 'table-sm']

    def sort_url(self, col_key, reverse=False):
        if reverse:
            direction =  'desc'
        else:
            direction = 'asc'
        return url_for('index', sort=col_key, direction=direction)


@app.route('/reprocess')
def reprocess():
    refdate = request.args.get('refdate')
    name = request.args.get('name')
    url = 'https://us-central1-kyd-storage-001.cloudfunctions.net/kyd-generic-download-publish?filter={}&refdate={}'.format(name, refdate)
    res = requests.get(url)


@app.route('/')
def index():
    key = client.key('Session', 'date')
    date = client.get(key)
    if request.args.get('date'):
        date = request.args.get('date')
        date = datetime.strptime(date, '%Y-%m-%d')
        date = SP_TZ.localize(date)
        logging.info('Request date %s', date)
    elif date:
        date = date['value']
        date = date.astimezone(SP_TZ)
        logging.info('Datastore date %s', date)
    else:
        date = datetime.now(SP_TZ) + timedelta(-1)
        logging.info('Now date %s', date)
    session_entry = datastore.Entity(key)
    session_entry.update({'value': date.astimezone(UTC)})
    client.put(session_entry)

    t1 = date.replace(hour=0, minute=0, second=0, microsecond=0)
    t2 = date.replace(hour=23, minute=59, second=59, microsecond=999999)
    t1 = t1.astimezone(UTC)
    t2 = t2.astimezone(UTC)
    logging.info('%s %s %s', date, t1, t2)

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
        else:
            log['filename'] = ''
    
    key = client.key('Session', 'status')
    status = client.get(key)
    if request.args.getlist('status'):
        status = request.args.getlist('status')
        status = [int(s) for s in status]
    elif status:
        status = status['value']
    else:
        status = [-1, 0, 1, 2]
    session_entry = datastore.Entity(key)
    session_entry.update({'value': status})
    client.put(session_entry)

    logs = [l for l in logs if l['status'] in status]
    sort = request.args.get('sort', 'name')
    reverse = (request.args.get('direction', 'asc') == 'desc')
    logs_sorted = logs.copy()
    logs_sorted.sort(key=lambda x: x[sort], reverse=reverse)
    tb = DownloadLogTable(logs_sorted, sort_by=sort, sort_reverse=reverse)

    return render_template('base.html',
        date=date,
        length=len(logs_sorted),
        status=status,
        table=tb,
        sort=sort,
        direction=request.args.get('direction', 'asc'))


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)