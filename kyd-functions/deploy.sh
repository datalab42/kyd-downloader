gcloud functions deploy kyd-generic-download \
    --entry-point gcf_generic_download \
    --runtime python37 \
    --trigger-topic kyd-storage-config \
    --env-vars-file .env.yaml \
    --project=kyd-storage-001

gcloud functions deploy kyd-generic-download-publish \
    --entry-point gcf_ignite_generic_downloads \
    --runtime python37 \
    --trigger-http \
    --env-vars-file .env.yaml \
    --project=kyd-storage-001

