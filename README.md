to do in glue job :"src_prefix": "s3://bi-efs-${SYS_LEVEL}-us-east-1-dna-raw-sf-ans/..."

The loader replaces ${SYS_LEVEL} with the Glue job param.

Masking rule:

If sensitivity = "pii" → mask

If sensitivity = "phi" → mask

If sensitivity = "confidential" → mask

If "none" → leave as-is

Simple rule:

pii → "********"
phi → "#####"
confidential → "XXX-CONFIDENTIAL-XXX"
