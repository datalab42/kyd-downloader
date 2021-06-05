
import os
import re
import os.path
import logging
import tempfile
import zipfile
from datetime import datetime, timedelta, date, timezone
from abc import ABC, abstractmethod
import json
import bizdays

import pytz
import requests

def downloader_factory(**kwargs):
    if kwargs.get('type') is None:
        return RawURLDownloader(**kwargs)
    elif kwargs.get('type') == 'datetime':
        return FormatDateURLDownloader(**kwargs)
    elif kwargs.get('type') == 'prepared':
        return PreparedURLDownloader(**kwargs)
    elif kwargs.get('type') == 'fundos_inf_diario':
        return FundosInfDiarioDownloader(**kwargs)
    elif kwargs.get('type') == 'b3files':
        return B3FilesURLDownloader(**kwargs)
    else:
        raise ValueError('Invalid downloader type %s', kwargs.get('type'))


def download_by_config(config_data, save_func, refdate=None):
    logging.info('content size = %d', len(config_data))
    config = json.loads(config_data)
    logging.info('content = %s', config)
    downloader = downloader_factory(**config)
    download_time = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f%z')
    logging.info('Download weekdays %s', config.get('download_weekdays'))
    if config.get('download_weekdays') and downloader.now.weekday() not in config.get('download_weekdays'):
        logging.info('Not a date to download. Weekday %s Download Weekdays %s', downloader.now.weekday(), config.get('download_weekdays'))
        msg = 'Not a date to download. Weekday {} Download Weekdays {}'.format(downloader.now.weekday(), config.get('download_weekdays'))
        return {
            'message': msg,
            'download_status': -1,
            'status': -1,
            'refdate': None,
            'filename': None,
            'bucket': config['output_bucket'],
            'name': config['name'],
            'time': download_time
        }
    try:
        fname, tfile, status_code, refdate = downloader.download(refdate=refdate)
        logging.info('Download time (UTC) %s', download_time)
        logging.info('Refdate %s', refdate)
        if status_code == 200:
            save_func(config, fname, tfile)
            msg = 'File saved'
            status = 0
        else:
            msg = 'File not saved'
            status = 1
        return {
            'message': msg,
            'download_status': status_code,
            'status': status,
            'refdate': refdate and refdate.strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
            'filename': fname,
            'bucket': config['output_bucket'],
            'name': config['name'],
            'time': download_time
        }
    except Exception as ex:
        logging.error(str(ex))
        return {
            'message': str(ex),
            'download_status': -1,
            'status': 2,
            'refdate': None,
            'filename': None,
            'bucket': config['output_bucket'],
            'name': config['name'],
            'time': download_time
        }


def save_file_to_temp_folder(attrs, fname, tfile):
    fname = '/tmp/{}'.format(fname)
    logging.info('saving file %s', fname)
    os.makedirs(os.path.dirname(fname), exist_ok=True)
    with open(fname, 'wb') as f:
        f.write(tfile.read())


class SingleDownloader(ABC):
    def __init__(self, **kwargs):
        self.attrs = kwargs
        self._url = None

    @property
    def now(self):
        tz = pytz.timezone('America/Sao_Paulo')
        return datetime.now(tz)

    @property
    def url(self):
        return self._url

    @abstractmethod
    def download(self, refdate=None):
        pass

    def get_fname(self, fname, refdate):
        prefix = self.attrs.get('prefix')
        if fname:
            fname = '{}/{}'.format(prefix, fname) if prefix else fname
        else:
            ext = self.attrs.get('ext', 'html')
            _date = refdate.strftime(self.attrs.get('fname_format', '%Y-%m-%d'))
            fname = '{}/{}.{}'.format(prefix, _date, ext) if prefix else '{}.{}'.format(_date, ext)
        return fname


class RawURLDownloader(SingleDownloader):
    def download(self, refdate=None):
        self._url = self.attrs['url']
        _, tfile, status_code, res = download_url(self._url)
        if status_code != 200:
            return None, None, status_code, None
        fname = self.get_fname(None, self.now)
        return fname, tfile, status_code, None


class FormatDateURLDownloader(SingleDownloader):
    def download(self, refdate=None):
        refdate = refdate or self.now + timedelta(self.attrs.get('timedelta', 0))
        logging.debug("REFDATE %s", refdate)
        logging.debug("SELF NOW %s", self.now)
        logging.debug("TIMEDELTA %s", self.attrs.get('timedelta', 0))
        self._url = refdate.strftime(self.attrs['url'])
        _, tfile, status_code, res = download_url(self._url)
        if status_code != 200:
            return None, None, status_code, refdate
        if self.attrs.get('use_filename'):
            _, fname = os.path.split(self._url)
        elif self.attrs.get('use_content_disposition'):
            cd = res.headers['content-disposition']
            fname = re.findall('filename="(.+)";', cd)[0]
        else:
            _, ext = os.path.splitext(self._url)
            ext = ext if ext != '' else '.{}'.format(self.attrs['ext'])
            fname = '{}{}'.format(refdate.strftime('%Y-%m-%d'), ext)
        f_fname = self.get_fname(fname, refdate)
        return f_fname, tfile, status_code, refdate


def get_date(dt):
    return dt.date() if isinstance(dt, datetime) else dt


def get_month(dt, monthdelta):
    first = date(dt.year, dt.month, 1)
    delta = 0
    while delta > monthdelta:
        first += timedelta(-1)
        dt = first
        first = date(dt.year, dt.month, 1)
        delta -= 1
    return first


class FundosInfDiarioDownloader(SingleDownloader):
    def download(self, refdate=None):
        refdate = refdate or self.now + timedelta(self.attrs.get('timedelta', 0))
        refmonth = get_month(refdate, self.attrs['month_reference'])
        self._url = refmonth.strftime(self.attrs['url'])
        _, tfile, status_code, res = download_url(self._url)
        if status_code != 200:
            return None, None, status_code, refdate
        _, ext = os.path.splitext(self._url)
        ext = ext if ext != '' else '.{}'.format(self.attrs['ext'])
        fname = '{}/{}{}'.format(refmonth.strftime('%Y-%m'), refdate.strftime('%Y-%m-%d'), ext)
        f_fname = self.get_fname(fname, refdate)
        return f_fname, tfile, status_code, refdate


class PreparedURLDownloader(SingleDownloader):
    def download(self, refdate=None):
        url = self.attrs['url']
        refdate = refdate or self.now + timedelta(self.attrs.get('timedelta', 0))
        params = {}
        for param in self.attrs['parameters']:
            param_value = self.attrs['parameters'][param]
            if isinstance(param_value, dict):
                if param_value['type'] == 'datetime':
                    params[param] = refdate.strftime(param_value['value'])
            else:
                params[param] = param_value

        self._url = url.format(**params)
        fname, temp_file, status_code = self._download_unzip_historical_data(self._url)
        if status_code != 200:
            return None, None, status_code, refdate
        f_fname = self.get_fname(fname, refdate)
        return f_fname, temp_file, status_code, refdate

    def _download_unzip_historical_data(self, url):
        _, temp, status_code, res = download_url(url)
        if status_code != 200:
            return None, None, status_code
        zf = zipfile.ZipFile(temp)
        nl = zf.namelist()
        if len(nl) == 0:
            logging.error('zip file is empty url = {}'.format(url))
            return None, None, 204
        name = nl[0]
        content = zf.read(name) # bytes
        zf.close()
        temp.close()
        temp = tempfile.TemporaryFile()
        temp.write(content)
        temp.seek(0)
        return name, temp, status_code


class B3FilesURLDownloader(SingleDownloader):
    calendar = bizdays.Calendar.load('ANBIMA.cal')
    def download(self, refdate=None):
        filename = self.attrs.get('filename')
        refdate = refdate or self.calendar.offset(self.now, self.attrs.get('offset', 0))
        logging.info('refdate %s', refdate)
        date = refdate.strftime('%Y-%m-%d')
        url = f'https://arquivos.b3.com.br/api/download/requestname?fileName={filename}&date={date}&recaptchaToken='
        res = requests.get(url)
        msg = 'status_code = {} url = {}'.format(res.status_code, url)
        logg = logging.warn if res.status_code != 200 else logging.info
        logg(msg)
        if res.status_code != 200:
            return None, None, res.status_code, refdate
        ret = res.json()
        url = f'https://arquivos.b3.com.br/api/download/?token={ret["token"]}'
        fname, temp_file, status_code, res = download_url(url)
        if res.status_code != 200:
            return None, None, res.status_code, refdate
        f_fname = self.get_fname(fname, refdate)
        return f_fname, temp_file, status_code, refdate


def download_url(url):
    res = requests.get(url)
    msg = 'status_code = {} url = {}'.format(res.status_code, url)
    logg = logging.warn if res.status_code != 200 else logging.info
    logg(msg)
    if res.status_code != 200:
        return None, None, res.status_code, res
    temp = tempfile.TemporaryFile()
    temp.write(res.content)
    temp.seek(0)
    return None, temp, res.status_code, res

