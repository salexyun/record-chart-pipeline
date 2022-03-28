# Running Airflow in Docker

## Prerequisites
1. Rename your GCP service account credential file to `google_credentials.json` and move it to `$HOME` directory
    ```shell
    cd ~ && mkdir -p ~/.google/credentials/
    mv <path/to/your/service-account-key>.json ~/.google/credentials/google_credentials.json
    ```
2. `docker-compose` v2.x+
3. `python` v3.7+

## Execution
1. Rebuild your `.env` file:
    ```shell
    mv .env_example .env
    ```
    * Set variables `GCP_PROJECT_ID` and `GCS_BUCKET_NAME` according to your configuration
2. Build the Docker image:
   ```shell
   docker-compose build
   ```
3. Start all serbices from the container
   ```shell
   docker-compose up
   ```
4. To ensure the containers and up and running:
   ```shell
   docker-compose ps
   ```
5. Login to the Airflow UI on `localhost:8080` with the default credential: `airflow/airflow`
6. Run the DAG
7. Upon completing the DAG run, stop the containers:
   ```
   docker-compose down
   ```

## BigQuery partitioning & clustering
* All of the tables were partitioned and clustered to optimize storage and performance; in particular:
  * `acoustic_features` table was partitioned by date (i.e., album_release_date) and clustered by album
    * It naturally makes sense to partition the table by date. This date, however, includes day and month as well. Given that the dataset includes 50+ years worth of data, I believe this level of granularity is not required for the upstream queries.
    * Similarly, clustering the table by album makes sense as many tracks can be grouped into an album. Alternatively, the table could have been clustered by artist instead.
  * `albums` table was partitioned by date (i.e., Billboard chart week)
    * See above explanation.
  * `segments` table was clustered by album
    * See above explanation.