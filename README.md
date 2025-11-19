
Run from CLI
aws glue start-job-run \
  --job-name financial_glue_pipeline \
  --arguments '{"--SYS_LEVEL":"dev","--DATASET":"VGI_FIN_GL_ACCOUNT_D","--FULL_LOAD":"false"}'
