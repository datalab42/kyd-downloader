from datetime import datetime, timedelta, timezone

from flask import Flask
from flask import render_template

from google.cloud import datastore


# If `entrypoint` is not defined in app.yaml, App Engine will look for an app
# called `app` in `main.py`.
app = Flask(__name__)
client = datastore.Client()


@app.route('/')
def index():
    q = client.query(kind='DownloadLog')
    t = datetime.utcnow() + timedelta(-2)
    t = t.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
    print(t)
    # receber a data no timezone de Sao_Paulo e converter para UTC neste range
    q.add_filter('time', '>=', t)
    q.add_filter('time', '<=', t+timedelta(1))
    logs = list(q.fetch())
    print(len(logs))
    return render_template('base.html', logs=logs)


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)