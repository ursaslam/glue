
name: Glue ETL Job CI

on:
  workflow_dispatch:
  push:
    branches: ["**"]
    tags-ignore: ["**"]

concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}

jobs:

  install:
    name: Install Dependencies
    uses: as/python.workflow/.github/workflows/pip-install.yml@v2
    with:
      pythonVersion: 3.11

  test:
    name: Run Tox
    uses: as/python.workflow/.github/workflows/tox.yml@v2
    needs: install
    with:
      pythonVersion: 3.11
      imageName: "asde-pyspark-glue/vg-pyspark-glue5:latest"

  package:
    name: Package Glue Job Artifact
    needs: [install, test]
    uses: as/glue.workflow/.github/workflows/package-glue-app.yml@v2
    with:
      custom-glue-libs-name: "custom"
      python-pip-artifact-name: ${{ needs.install.outputs.artifactName }}

  zip:
    name: Create Glue Job ZIP
    needs: package
    runs-on: self-hosted
    steps:
      - name: Checkout source
        uses: actions/checkout@v4

      - name: Create Glue Job ZIP (include EVERYTHING under src/app)
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

  prepare:
    name: Prepare Glue Job Artifact for S3 Upload
    needs: zip
    uses: as/s3-upload.workflow/.github/workflows/ci.yml@v1
    with:
      artifactName: "glue_job_package.zip"
      artifactDestination: "./src/app/"
      filesToUpload: |
        ./src/app/
        ./src/app/custom/
        ./src/app/custom/config/

  create-release:
    if: github.ref_name == 'main'
    name: Create GitHub Release
    needs: prepare
    uses: as/release-generator.workflow/.github/workflows/release.yml@v1
