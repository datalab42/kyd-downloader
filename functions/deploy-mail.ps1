. ..\..\Setenv.ps1

gcloud functions deploy kyd-sendmail --entry-point gcf_sendmail --runtime python37 --trigger-http --env-vars-file .env.yaml --project=kyd-storage-001

gcloud functions deploy kyd-sendmail --update-env-vars SENDGRID_API_KEY=$env:SENDGRID_API_KEY --project=kyd-storage-001
