
import json
import tempfile
from google.cloud import storage
import textparser


def handle_csv(file, sep=';', encoding='utf-8'):
    parser = textparser.GenericParser()
    for ix, line in enumerate(file):
        line = line.decode(encoding).strip()
        if ix == 0:
            hdr = [field.lower() for field in line.split(sep)]
        vals = [parser.parse(val) for val in line.split(sep)]
        yield dict(zip(hdr, vals))


storage_client = storage.Client()
bucket = storage_client.get_bucket('ks-tmp')
blob = bucket.get_blob('inf_diario_fi_202005.csv')
temp = tempfile.TemporaryFile('wb+')
blob.download_to_file(temp)
temp.seek(0)

for ix, data in enumerate(handle_csv(temp)):
    if ix > 10:
        break
    print(json.dumps(data))

temp.close()