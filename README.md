
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
    uses: as/python.workflow/.github/workflows/pip-install.yml@v2
    with:
      pythonVersion: 3.11

  # ---------------------------------------------------------
  # 2. RUN TESTS
  # ---------------------------------------------------------
  test:
    name: Run Tox
    needs: install
    uses: as/python.workflow/.github/workflows/tox.yml@v2
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
  # 4. ZIP CUSTOM FOLDER FOR GLUE JOB
  # ---------------------------------------------------------
  zip:
    name: Create custom.zip for Glue Job
    needs: package
    runs-on: self-hosted

    steps:

      # 4.1 Checkout your source repo
      - name: Checkout repo
        uses: actions/checkout@v4

      # 4.2 Download Python package generated in previous stage
      - name: Download packaged artifact
        uses: actions/download-artifact@v4
        with:
          name: ${{ needs.package.outputs.artifact-name }}
          path: src/app

      # 4.3 Debug
      - name: Debug src/app structure
        run: ls -R src/app

      # 4.4 Create custom.zip (only custom folder)
      - name: Create custom.zip
        run: |
          cd src/app
          zip -r ../../custom.zip custom
          cd ../..
          echo "ZIP CONTENTS:"
          unzip -l custom.zip

      # 4.5 Upload custom.zip to next step
      - name: Upload custom.zip
        uses: actions/upload-artifact@v4
        with:
          name: custom-zip
          path: custom.zip

  # ---------------------------------------------------------
  # 5. UPLOAD TO S3
  # ---------------------------------------------------------
  prepare:
    name: Upload custom.zip to S3
    needs: zip
    uses: v/s3-upload.workflow/.github/workflows/ci.yml@v1
    with:
      artifactName: custom-zip
      artifactDestination: s3://your-bucket-name/glue/custom/
      filesToUpload: custom.zip

  # ---------------------------------------------------------
  # 6. CREATE RELEASE (MAIN ONLY)
  # ---------------------------------------------------------
  create-release:
    if: github.ref_name == 'main'
    name: Release Branch
    needs: prepare
    uses: v/release-generator.workflow/.github/workflows/release.yml@v1
