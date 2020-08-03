
import os
import csv
import json
import base64
import logging
import tempfile
from datetime import datetime
from itertools import dropwhile
from io import StringIO

import lxml.html
import pytz

from google.cloud import storage
from google.cloud import pubsub_v1
from google.cloud import datastore

from kyd.data.downloaders import downloader_factory, download_by_config
from kyd.data.logs import save_download_logs


# TODO: this function saves the file to bucket based on its configuration
# this might be thought as an object
def save_file_to_output_bucket(attrs, fname, tfile):
    output_bucket_name = attrs['output_bucket']
    _save_file_to_bucket(output_bucket_name, fname, tfile)


def _save_file_to_bucket(output_bucket_name, fname, tfile):
    logging.info('saving file %s/%s', output_bucket_name, fname)
    storage_client = storage.Client()
    output_bucket = storage_client.get_bucket(output_bucket_name)
    output_blob = output_bucket.blob(fname)
    output_blob.upload_from_file(tfile)


def _save_text_to_bucket(output_bucket_name, fname, text):
    logging.info('saving file %s/%s', output_bucket_name, fname)
    storage_client = storage.Client()
    output_bucket = storage_client.get_bucket(output_bucket_name)
    output_blob = output_bucket.blob(fname)
    output_blob.upload_from_string(text)


# ----
# endpoint functions
# ----
def gcf_generic_download(event, context):
    input_data = base64.b64decode(event['data']).decode('utf-8')
    try:
        # TODO: this is not good
        # this function download_by_config must be split
        # the settings to save the file must be handled here
        logging.info('Attributes %s', event['attributes'])
        if event['attributes'] and event['attributes'].get('refdate'):
            refdate = datetime.strptime(event['attributes'].get('refdate'), '%Y-%m-%d')
            refdate = pytz.timezone('America/Sao_Paulo').localize(refdate)
            refdate = refdate.replace(microsecond=1)
            res = download_by_config(input_data, save_file_to_output_bucket, refdate=refdate)
        else:
            res = download_by_config(input_data, save_file_to_output_bucket)
        topic_name = get_env_var('DOWNLOAD_LOG_TOPIC')
        publisher = pubsub_v1.PublisherClient()
        future = publisher.publish(topic_name, bytes(json.dumps(res), 'utf-8'))
        res = future.result()
        logging.info('DownloadLog message sent to pubsub %s', res)
    except Exception as ex:
        logging.error(ex)


def get_env_var(var_name):
    var_value = os.environ.get(var_name)
    if var_value is None:
        raise ValueError(f'{var_name} env var not set')
    return var_value


def gcf_ignite_generic_downloads(request):
    bucket_name = get_env_var('CONFIG_BUCKET')
    topic_name = get_env_var('CONFIG_TOPIC')
    filter_prefix = get_env_var('CONFIG_PREFIX')

    storage_client = storage.Client()
    config_bucket = storage_client.get_bucket(bucket_name)
    blobs = list(config_bucket.list_blobs(prefix=filter_prefix))

    _filter = request.args.get('filter')
    _refdate = request.args.get('refdate')
    publisher = pubsub_v1.PublisherClient()
    for blob in blobs:
        if blob.path.endswith('%2F'):
            continue
        if _filter and _filter not in blob.path:
            continue
        logging.info('publishing %s', blob.path)
        if _refdate:
            publisher.publish(topic_name, blob.download_as_string(), refdate=_refdate)
        else:
            publisher.publish(topic_name, blob.download_as_string())


def gcf_save_download_logs(event, context):
    input_data = base64.b64decode(event['data']).decode('utf-8')
    try:
        data = json.loads(input_data)
        save_download_logs(data)
    except Exception as ex:
        logging.error(ex)

