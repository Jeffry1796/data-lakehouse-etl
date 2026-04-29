import os
import pandas as pd
from datetime import datetime
from typing import List, Tuple
from google.cloud import storage
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class NYCTaxiIngestion:
    """Download and ingest NYC Taxi dataset to GCS"""

    # NYC Taxi data URLs (Yellow taxi)
    YELLOW_TAXI_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
    GREEN_TAXI_URL  = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month:02d}.parquet"
    TAXI_ZONES_URL  = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

    def __init__(self, project_id, bucket_name, gcs_credentials_path):
        """
        Initialize NYC Taxi Ingestion

        Args:
            project_id: GCP Project ID
            bucket_name: GCS bucket name (e.g., 'data-project-bronze-xxx')
            gcs_credentials_path: Path to GCP service account JSON (optional)
        """
        self.project_id = project_id
        self.bucket_name = bucket_name

        if gcs_credentials_path:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = gcs_credentials_path

        self.storage_client = storage.Client(project=project_id)
        self.bucket = self.storage_client.bucket(bucket_name+'-'+project_id)

    def download_dataset(self, dataset_type, start_year, start_month, end_year, end_month):
        """
        Download NYC taxi data for specified period

        Args:
            dataset_type: 'yellow' or 'green'
            start_year: Starting year
            start_month: Starting month (1-12)
            end_year: Ending year
            end_month: Ending month (1-12)
        """
        datasets = []
        url_template = self.YELLOW_TAXI_URL if dataset_type == "yellow" else self.GREEN_TAXI_URL

        current_year = start_year
        current_month = start_month

        while (current_year < end_year) or (current_year == end_year and current_month <= end_month):
            url = url_template.format(year=current_year, month=current_month)
            logger.info(f"Downloading {dataset_type} taxi data: {current_year}-{current_month:02d}")

            try:
                df = pd.read_parquet(url)
                logger.info(f"Successfully downloaded {len(df)} rows for {current_year}-{current_month:02d}")
                datasets.append((df, current_year, current_month))

            except Exception as e:
                logger.warning(f"Failed to download {dataset_type} taxi for {current_year}-{current_month:02d}: {e}")

            # Increment month
            current_month += 1
            if current_month > 12:
                current_month = 1
                current_year += 1

        return datasets

    def upload_to_gcs(self, dataframe, taxi_type, year, month, file_format):
        """
        Upload dataframe to GCS with partition structure

        Args:
            dataframe: Dataframe to upload
            taxi_type: 'yellow_taxi' or 'green_taxi'
            year: Year
            month: Month
            file_format: 'parquet' or 'csv'

        Returns:
            GCS path of uploaded file
        """
        # Create partition path: bronze/{taxi_type}/year={year}/month={month}/
        partition_path = f"{taxi_type}/year={year}/month={month:02d}"
        filename = f"{taxi_type}_{year}-{month:02d}.{file_format}"
        gcs_path = f"{partition_path}/{filename}"

        logger.info(f"Uploading to GCS: gs://{self.bucket_name}-{self.project_id}/{gcs_path}")

        try:
            # Upload file
            blob = self.bucket.blob(gcs_path)

            if file_format == "parquet":
                blob.upload_from_string(
                    dataframe.to_parquet(index=False),
                    content_type="application/octet-stream"
                )
            else:  # csv
                blob.upload_from_string(
                    dataframe.to_csv(index=False),
                    content_type="text/csv"
                )

            logger.info(f"Successfully uploaded: gs://{self.bucket_name}/{gcs_path}")
            return gcs_path

        except Exception as e:
            logger.error(f"Failed to upload {gcs_path}: {e}")
            raise

    def download_taxi_zones(self) -> pd.DataFrame:
        """Download taxi zones reference data"""
        logger.info("Downloading taxi zones data...")

        try:
            df = pd.read_csv(self.TAXI_ZONES_URL)
            logger.info(f"Successfully downloaded {len(df)} taxi zones")
            return df

        except Exception as e:
            logger.error(f"Failed to download taxi zones: {e}")
            raise

    def upload_taxi_zones_to_gcs(self, dataframe) -> str:
        """Upload taxi zones to GCS"""
        gcs_path = "taxi_zones/taxi_zones.csv"

        logger.info(f"Uploading taxi zones to GCS: gs://{self.bucket_name}/{gcs_path}")

        try:
            blob = self.bucket.blob(gcs_path)
            blob.upload_from_string(
                dataframe.to_csv(index=False),
                content_type="text/csv"
            )

            logger.info(f"Successfully uploaded taxi zones: gs://{self.bucket_name}/{gcs_path}")
            return gcs_path

        except Exception as e:
            logger.error(f"Failed to upload taxi zones: {e}")
            raise

    def ingest_all(self, taxi_types, start_year, start_month, end_year, end_month, include_zones):
        """
        Run full ingestion pipeline
        Args:
            taxi_types: List of ['yellow'] or ['green'] or ['yellow', 'green']
            start_year: Starting year
            start_month: Starting month
            end_year: Ending year
            end_month: Ending month
            include_zones: Whether to download and upload taxi zones
        """
        logger.info("Starting NYC Taxi dataset ingestion...")
        logger.info(f"Bucket: gs://{self.bucket_name}")

        uploaded_files = []

        # Ingest taxi data
        for taxi_type in taxi_types:
            logger.info(f"\n=== Ingesting {taxi_type.upper()} Taxi Data ===")

            datasets = self.download_dataset(
                dataset_type=taxi_type,
                start_year=start_year,
                start_month=start_month,
                end_year=end_year,
                end_month=end_month
            )

            for df, year, month in datasets:
                gcs_path = self.upload_to_gcs(
                    df,
                    taxi_type=f"{taxi_type}_taxi",
                    year=year,
                    month=month,
                    file_format="parquet"
                )
                uploaded_files.append(gcs_path)

        # Ingest taxi zones
        if include_zones:
            logger.info("\n=== Ingesting Taxi Zones Reference Data ===")
            zones_df = self.download_taxi_zones()
            gcs_path = self.upload_taxi_zones_to_gcs(zones_df)
            uploaded_files.append(gcs_path)

        logger.info(f"\n✅ Ingestion completed! {len(uploaded_files)} files uploaded:")
        for path in uploaded_files:
            logger.info(f"  - gs://{self.bucket_name}/{path}")

        return uploaded_files