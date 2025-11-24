to do in glue job :"src_prefix": "s3://bi-efs-${SYS_LEVEL}-us-east-1-dna-raw-sf-ans/..."

The loader replaces ${SYS_LEVEL} with the Glue job param.

👉 Iceberg itself writes Parquet files into the LOCATION you passed (table_location, i.e. curated S3 path).
You don’t manually call df.write.parquet when using Iceberg; you insert/merge into the table and Iceberg writes Parquet under the hood.
Masking rule:

If sensitivity = "pii" → mask

If sensitivity = "phi" → mask

If sensitivity = "confidential" → mask

If "none" → leave as-is

Simple rule:

pii → "********"
phi → "#####"
confidential → "XXX-CONFIDENTIAL-XXX"
