
Run from CLI
aws glue start-job-run \
  --job-name financial_glue_pipeline \
  --arguments '{"--SYS_LEVEL":"dev","--DATASET":"VGI_FIN_GL_ACCOUNT_D","--FULL_LOAD":"false"}'


Verifying Iceberg Table Creation

After job runs, verify:
Check Glue Catalog
Database:
adw_erp_spen

Table:
vgi_fin_gl_account_d

Query in Athena using Iceberg engine
SELECT COUNT(*) 
FROM glue_catalog.adw_erp_spen.vgi_fin_gl_account_d;
