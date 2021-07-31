# dwh-dataengineeringnd
In this project, we try to help a Music service work in analytics infrastracture build on AWS with the following ETL pipeline:
- Extract JSON log data from S3.
- Transform and load extracted table data into Redshift as staging table.
- Convert the staging table into star schema optimized for played songs analytics.

## AWS setup

1. Create IAM role.
  - Attach at least the following policy for this project:
    - `AmazonS3ReadOnlyAccess`

2. Select region where the S3 bucket exists.
  - `us-west-2` for `udacity-dend` bucket
  - This is necessary for Redshift to excute COPY command from S3.

3. Create security group.
  - Add inbound rule opening the port number which Redshift uses.

4. Create Redshift cluster.
  - Apply security group created above.
  - Associate IAM role created above.
  - Enable public access.

5. Create an IAM User.
  - Attach at least the following policies for this project:
    - `AmazonRedshiftFullAccess`
    - `AmazonS3ReadOnlyAccess`

6. Create an EC2 instance.
  - To avoid timeout, ETL script should be executed in the same region.
  - Install some dependencies: python3, postgressql (libpq-dev), psycopg2.

## ETL pipeline excution

1. Fill in the Redshift environment information in the config file (`dwh.cfg`)

2. `$python create_tables.py`
  - Initialize tables: DROP tables if any, and CREATE tables with specified schema.
  - DROP and CREATE queries are defined in `sql_queries.py`.

3. `$python etl.py`
  - Extract JSON files from S3 and create staging tables from them.
  - And then convert staging tables to star schema.
  - COPY and INSERT queries are also defined in `sql_queries.py`.

## Database Schema

1. Staging tables
  - `staging_events`: Log data which contains music play events genereted by service users.
  - `staging_songs`: Music data which includes song itself and its artist information.

2. Star schema tables
  - Fact table:
    - `songplays`: Data on the events in which the user played the music. This aggregates information in dimension tables.
  - Dimension tables:
    - `users`: Data on users who played the music derived from `staging_events`.
    - `songs`: Data on songs derived from `staging_songs`.
    - `artists`: Data on artists of songs derived from `staging_songs`.
    - `time`: Data about when and how long the user played the music.