
gcloud functions deploy kyd-generic-download --entry-point gcf_generic_download --runtime python37 --trigger-topic kyd-storage-config --env-vars-file .env.yaml --project=kyd-storage-001

gcloud functions deploy kyd-generic-download-publish --entry-point gcf_ignite_generic_downloads --runtime python37 --trigger-http --env-vars-file .env.yaml --project=kyd-storage-001

gcloud functions deploy kyd-save-download-logs --entry-point gcf_save_download_logs --runtime python37 --trigger-topic kyd-storage-download-log --env-vars-file .env.yaml --project=kyd-storage-001

gcloud functions deploy kyd-sendmail --entry-point gcf_sendmail --runtime python37 --trigger-http --env-vars-file .env.yaml --project=kyd-storage-001 --set-env-vars SENDGRID_API_KEY=$env:SENDGRID_API_KEY

gcloud functions deploy kyd-sendmail --update-env-vars SENDGRID_API_KEY=$env:SENDGRID_API_KEY --project=kyd-storage-001 
