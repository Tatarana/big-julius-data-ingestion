"""Application configuration via pydantic-settings.

Loads all required environment variables for the service.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables.

    Attributes:
        aws_access_key_id: AWS access key ID for S3 authentication.
        aws_secret_access_key: AWS secret access key for S3 authentication.
        aws_region: AWS region where the S3 bucket is hosted.
        s3_bucket_name: Name of the S3 bucket containing CSV files.
        s3_prefix: Folder path (prefix) inside the S3 bucket.
        google_application_credentials: Path to the Google service account JSON file.
        firestore_project_id: Google Cloud project ID for Firestore.
        collection_main: Name of the main Firestore collection for transactions.
        collection_temp: Name of the temporary Firestore collection used during ingestion.
        log_level: Logging level for the application.
    """

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    aws_access_key_id: str
    aws_secret_access_key: str
    aws_region: str
    s3_bucket_name: str
    s3_prefix: str
    google_application_credentials: str
    firestore_project_id: str
    firestore_database_id: str = "(default)"
    collection_main: str = "transactions"
    collection_temp: str = "transactions_temp"
    collection_rules: str = "classification_rules"
    log_level: str = "INFO"


settings = Settings()
