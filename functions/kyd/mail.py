
import os
import logging

import pytz
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from google.cloud import datastore

client = datastore.Client()
SP_TZ = pytz.timezone('America/Sao_Paulo')
UTC = pytz.utc


HEADER = {
    'name': 'Name',
    'time': 'Time',
    'refdate': 'Refdate',
    'download_status': 'DownloadStatus',
    'status': 'Status',
    'filename': 'Filename',
    'message': 'Message',
}

def gentable(data):
    table = '<table width="100%" style="border:1px solid #333">\n'
    # Create the table's column headers
    keys = list(HEADER.keys())
    hdr = [HEADER[h] for h in keys]
    table += '  <tr>\n'
    for column in hdr:
        table += '    <th align="center">{0}</th>\n'.format(column.strip())
    table += '  </tr>\n'

    # Create the table's row data
    for line in data:
        table += '  <tr>\n'
        for column in keys:
            table += '    <td>{0}</td>\n'.format(line[column])
        table += '  </tr>\n'

    table += '</table>'

    return table

def getdata(date):
    # date = datetime.now(SP_TZ) + timedelta(-1)
    logging.info('Now date %s', date)
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
    
    logs_sorted = logs.copy()
    logs_sorted.sort(key=lambda x: x['download_status'], reverse=False)
    return gentable(logs_sorted)


def sendmail(date):
    tb = getdata(date)
    html_content = f'''
<!doctype html>
<html lang="en">
    <body>
        <h1>
            <a href="https://kyd-storage-001.rj.r.appspot.com/?date={date.strftime('%Y-%m-%d')}">
                kyd download report {date.strftime('%Y-%m-%d')}
            </a>
        </h1>
        <p>
            Date: {date.strftime('%Y-%m-%d')}
        </p>
        <div>
        {tb}
        </div>
        <p>
            <a href="https://kyd-storage-001.rj.r.appspot.com/?date={date.strftime('%Y-%m-%d')}">DownloadLog</a>
        </p>
    </body>
</html>
'''
    subject = f"kyd download report {date.strftime('%Y-%m-%d')}"

    message = Mail(
        from_email='wilson.freitas@gmail.com',
        to_emails='wilson.freitas@gmail.com',
        subject=subject,
        html_content=html_content)

    try:
        sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
        sg.send(message)
        logging.info('E-mail sent')
    except Exception as e:
        logging.error('Error sending e-mail sent')
        logging.error(e)


if __name__ == '__main__':
    import datetime
    sendmail(datetime.datetime(2021, 4, 22))
    # print(getdata(datetime.datetime(2021, 4, 21)).__html__())
