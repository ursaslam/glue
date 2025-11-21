zip:
    name: Zip Glue Job Including Custom Folder
    needs: package
    runs-on: ubuntu-latest
    steps:
      - name: Checkout Code
        uses: actions/checkout@v4

      - name: Create Glue Job ZIP (include EVERYTHING)
        run: |
          echo "Zipping entire src/app folder..."
          cd src/app
          zip -r ../../glue_job_package.zip .
          cd ../..
          echo "ZIP Contents:"
          unzip -l glue_job_package.zip

      - name: Upload ZIP for next stage
        uses: actions/upload-artifact@v4
        with:
          name: glue-zip
          path: glue_job_package.zip

          



Run from CLI
aws glue start-job-run \
  --job-name financial_glue_pipeline \
  --arguments '{"--SYS_LEVEL":"dev","--DATASET":"VGI_FIN_GL_ACCOUNT_D","--FULL_LOAD":"false"}'


Verifying Iceberg Table Creation:

After job runs, verify:
Check Glue Catalog
Database:
adw_erp_spen

Table:
vgi_fin_gl_account_d

Query in Athena using Iceberg engine
SELECT COUNT(*) 
FROM glue_catalog.adw_erp_spen.vgi_fin_gl_account_d;




zip:
  name: Create Glue Job ZIP
  needs: package
  runs-on: self-hosted

  steps:
    # 1. Checkout code (to get src/app)
    - name: Checkout code
      uses: actions/checkout@v4

    # 2. Download wheel from the package job (critical!)
    - name: Download packaged artifact
      uses: actions/download-artifact@v4
      with:
        name: ${{ needs.package.outputs.artifact-name }}
        path: dist/

    # 3. DEBUG: Show workspace (very important)
    - name: Debug workspace
      run: ls -R .

    # 4. Create ZIP from correct directory
    - name: Create Glue Job ZIP (include EVERYTHING in src/app)
      run: |
        echo "Zipping src/app into glue_job_package.zip"
        cd src/app
        zip -r ../../glue_job_package.zip .
        cd ../..
        echo "ZIP Contents:"
        unzip -l glue_job_package.zip

    # 5. Upload ZIP for next job
    - name: Upload ZIP for next stage
      uses: actions/upload-artifact@v4
      with:
        name: glue-zip
        path: glue_job_package.zip
