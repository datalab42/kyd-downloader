
import logging
from datetime import datetime, timedelta, timezone

import pytz
from flask import Flask
from flask import render_template
from flask import request, url_for
from flask_table import Table, Col
from flask_table.html import element
import requests

# from google.cloud import storage
from google.cloud import datastore
import google.oauth2.id_token
import google.auth.transport.requests

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


class DownloadLogTable(Table):
    name = Col('Name')
    time = Col('Time')
    refdate = Col('Refdate')
    download_status = Col('DownloadStatus')
    status = Col('Status')
    filename = Col('Filename')
    message = Col('Message')
    url = URLCol('URL')
    allow_sort = False
    table_id = 'data'
    classes = ['table', 'table-striped', 'table-sm']

    def sort_url(self, col_key, reverse=False):
        if reverse:
            direction =  'desc'
        else:
            direction = 'asc'
        return url_for('index', sort=col_key, direction=direction)


def get_id_token(url):
    _request = google.auth.transport.requests.Request()
    id_token = google.oauth2.id_token.fetch_id_token(_request, url)
    logging.info('token %s', id_token)
    return id_token


@app.route('/reprocess')
def reprocess():
    url = 'https://us-central1-kyd-storage-001.cloudfunctions.net/kyd-generic-download-publish'
    id_token = get_id_token(url)
    refdate = request.args.get('refdate')
    name = request.args.get('name')
    url = f'{url}?filter={name}&refdate={refdate}'
    res = requests.get(url, headers={'Authorization': f'bearer {id_token}'})
    return res.text, res.status_code


DEFAULT_TASKNAME = 'Todos'

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

    key = client.key('Session', 'taskname')
    taskname = client.get(key)
    if request.args.get('taskname'):
        taskname = request.args.get('taskname')
    elif taskname:
        taskname = taskname['value']
    else:
        taskname = DEFAULT_TASKNAME
    session_entry = datastore.Entity(key)
    session_entry.update({'value': taskname})
    client.put(session_entry)

    key = client.key('Session', 'compress')
    compress = client.get(key)
    if request.args.get('compress'):
        compress = request.args.get('compress') == '1'
        logging.info('compress from request %s', compress)
    else:
        compress = False
        logging.info('compress default %s', compress)

    q = client.query(kind='DownloadLog')
    q.projection = ['name']
    q.distinct_on = ['name']
    q.order = ['name']
    names = [n['name'] for n in q.fetch()]
    # logging.info(names)
    names.insert(0, DEFAULT_TASKNAME)

    q = client.query(kind='DownloadLog')
    if taskname != DEFAULT_TASKNAME:
        logging.info('taskname %s', taskname)
        q.add_filter('name', '=', taskname)
    else:    
        t1 = date.replace(hour=0, minute=0, second=0, microsecond=0)
        t2 = date.replace(hour=23, minute=59, second=59, microsecond=999999)
        t1 = t1.astimezone(UTC)
        t2 = t2.astimezone(UTC)
        logging.info('time %s %s %s', date, t1, t2)
        q.add_filter('time', '>=', t1)
        q.add_filter('time', '<=', t2)

    logs = list(q.fetch())
    for log in logs:
        if log['refdate']:
            log['refdate'] = log['refdate'].astimezone(SP_TZ).strftime('%Y-%m-%d')
        else:
            log['refdate'] = ''
        log['time'] = log['time'].astimezone(SP_TZ).strftime('%Y-%m-%d %H:%M:%S.%f')

        if log['filename']:
            log['filename'] = 'gs://{}/{}'.format(log['bucket'], log['filename'])
        else:
            log['filename'] = ''
        # storage_client = storage.Client()
        # bucket = storage_client.bucket(log['bucket'])
        # log['file_exists'] = storage.Blob(bucket=bucket, name=log['filename']).exists(storage_client)
        
        url = f'/reprocess?refdate={log["refdate"]}&name={log["name"]}'
        log['url'] = url
    
    if compress:
        check_ok = {}
        for log in logs:
            key = f'{log["name"]}-{log["refdate"]}'
            if log['status'] == 0:
                check_ok[key] = log
        for log in logs:
            key = f'{log["name"]}-{log["refdate"]}'
            if log['status'] != 0 and key not in check_ok:
                check_ok[key] = log
        logs = list(check_ok.values())
    
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
    logs.sort(key=lambda x: (x['refdate'], x['time']), reverse=True)

    return render_template('table.html',
        date=date,
        names=names,
        taskname=taskname,
        length=len(logs),
        status=status,
        compress=compress,
        logs=logs)


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)