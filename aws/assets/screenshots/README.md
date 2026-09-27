# Screenshots

Capture these during a demo (`make deploy` → `make simulate` → `make aggregate` → `make query`),
save them here with these exact names, and they will show up in `aws/README.md`:

| File | What to capture |
|---|---|
| `cloudwatch-dashboard.png` | CloudWatch dashboard `clockdata-dev` while `make simulate` runs |
| `athena-query.png` | Athena query editor with the result of `04_device_leaderboard` |
| `s3-datalake.png` | S3 console, data lake bucket showing `bronze/ silver/ gold/ quarantine/` |
| `alarm-email.png` | The SNS e-mail of the `abnormal-heart-rate` alarm (`make simulate ANOMALY_RATE=0.2`) |

Then run `make destroy`.
