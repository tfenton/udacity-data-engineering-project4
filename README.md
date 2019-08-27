# Introduction
Music streaming startup Sparkify, want to move their processes and data onto the cloud. Currently their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This project builds an ETL pipeline that extracts the data from S3 and stages it in a Spark cluster on AWS allowing the analytics team to continue finding insights in what songs their users are listening to.
# Setup
All configuration is performed in the dwh.cfg file. None of the credentials  included in this repository version. You will need to spin up a Spark EMR cluster in AWS and fill in the IAM creds in the config file prior to running the ETL pipeline

# ETL Pipepline
## Usage

```bash
python etl.py
```
At this point the data is populated and ready for analysis.
