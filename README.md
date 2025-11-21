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




name: Build & Deploy Glue Job

concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: false

jobs:

  # ---------------------------------------------------------
  # 1. INSTALL DEPENDENCIES
  # ---------------------------------------------------------
  install:
    name: Install Dependencies
    uses: v/python.workflow/.github/workflows/pip-install.yml@v2
    with:
      pythonVersion: 3.11

  # ---------------------------------------------------------
  # 2. RUN TESTS
  # ---------------------------------------------------------
  test:
    name: Run Tox
    needs: install
    uses: v/python.workflow/.github/workflows/tox.yml@v2
    with:
      pythonVersion: 3.11
      imageName: "vg-pyspark-glue/vg-pyspark-glue5:latest"

  # ---------------------------------------------------------
  # 3. PACKAGE GLUE APP (creates Python wheel)
  # ---------------------------------------------------------
  package:
    name: Package Glue Job Artifact
    needs: [install, test]
    uses: v/glue.workflow/.github/workflows/package-glue-app.yml@v2
    with:
      custom-glue-libs-name: "custom"
      python-pip-artifact-name: ${{ needs.install.outputs.artifactName }}

  # ---------------------------------------------------------
  # 4. ZIP CUSTOM FOLDER (src/app/custom) FOR GLUE JOBS
  # ---------------------------------------------------------
  zip:
    name: Create custom.zip for Glue Job
    needs: package
    runs-on: self-hosted

    steps:

      # 4.1 Checkout your source repository
      - name: Checkout repo
        uses: actions/checkout@v4

      # 4.2 Download the pip package created in previous step
      - name: Download packaged artifact
        uses: actions/download-artifact@v4
        with:
          name: ${{ needs.package.outputs.artifact-name }}
          path: src/app

      # 4.3 Debug structure (VERY IMPORTANT)
      - name: Debug src/app tree
        run: ls -R src/app

      # 4.4 Create custom.zip containing ONLY src/app/custom
      - name: Create custom.zip
        run: |
          echo "Zipping src/app/custom folder..."
          cd src/app
          zip -r ../../custom.zip custom
          cd ../..
          echo "ZIP CONTENTS:"
          unzip -l custom.zip

      # 4.5 Upload this zip to next stage
      - name: Upload custom.zip
        uses: actions/upload-artifact@v4
        with:
          name: custom-zip
          path: custom.zip

  # ---------------------------------------------------------
  # 5. UPLOAD TO S3 (GLUE CAN READ THIS DIRECTLY)
  # ---------------------------------------------------------
  prepare:
    name: Upload Glue Job ZIP to S3
    needs: zip
    uses: v/s3-upload.workflow/.github/workflows/ci.yml@v1
    with:
      artifactName: custom-zip
      artifactDestination: s3://your-bucket-name/glue/custom/
      filesToUpload: custom.zip

  # ---------------------------------------------------------
  # 6. OPTIONAL: CREATE RELEASE ON MAIN BRANCH
  # ---------------------------------------------------------
  create-release:
    if: github.ref_name == 'main'
    name: Release Branch
    needs: prepare
    uses: v/release-generator.workflow/.github/workflows/release.yml@v1
