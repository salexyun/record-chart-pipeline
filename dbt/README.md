# Data Transformation via dbt

## Prerequisites
1. Create a [service account](https://console.cloud.google.com/apis/credentials/wizard)
   * Select "BigQuery API" &rarr; "Application data" &rarr; "No, I'm not using them"
   * Name service account and account ID; give a brief description
2. Grant BigQuery Admin role in [IAM & Admin](https://console.cloud.google.com/iam-admin/iam)
   * Create a new private key; choose the JSON option and auth file will be downloaded automatically

## dbt Cloud setup
1. Create a [dbt account](https://www.getdbt.com/signup/)
2. Set up a new project
   * Name your project
   * Set up a database connection: choose BigQuery as the data warehouse
   * Upload the above GCP service key
   * Set up development credentials
   * Click on the "Test" button
3. Add GitHub repository
   * Copy the SSH key from your GitHub repo and paste it to the Repository section of dbt Cloud
     * Alternatively, simply connect your GitHub account to your dbt account (then you may skip the next instruction)
   * Copy the deploy key from dbt Cloud
   * In GitHub repo, "Settings" tab &rarr; "Deploy keys" section &rarr; paste above deploy key

## Execution
1. Run the models:
   ```shell
   dbt run
   ```
2. Test the data:
   ```shell
   dbt test
   ```
3. Generate documentation:
   ```shell
   dbt docs generate
   ```