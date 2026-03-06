"""Router for the /process-files ingestion endpoint."""

import logging

from fastapi import APIRouter, Depends, HTTPException, status

from app.models.transaction import IngestionResponse
from app.services.firestore_service import FirestoreService, FirestoreServiceError, build_firestore_client
from app.services.ingestion_service import IngestionService
from app.services.s3_service import S3Service, S3ServiceError, build_s3_client

logger = logging.getLogger(__name__)

router = APIRouter()


def get_ingestion_service() -> IngestionService:
    """Dependency provider for IngestionService using production clients.

    Returns:
        A fully configured IngestionService instance.
    """
    from app.core.config import settings  # noqa: PLC0415 — lazy to avoid import-time env validation

    s3_client = build_s3_client(
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
        aws_region=settings.aws_region,
    )
    s3_service = S3Service(
        client=s3_client,
        bucket=settings.s3_bucket_name,
        prefix=settings.s3_prefix,
    )
    firestore_client = build_firestore_client(
        project_id=settings.firestore_project_id,
        database=settings.firestore_database_id,
        credentials_path=settings.google_application_credentials,
    )
    firestore_service = FirestoreService(
        client=firestore_client,
        main_collection=settings.collection_main,
        temp_collection=settings.collection_temp,
        rules_collection=settings.collection_rules,
    )
    return IngestionService(s3_service=s3_service, firestore_service=firestore_service)


@router.post(
    "/process-files",
    response_model=IngestionResponse,
    status_code=status.HTTP_200_OK,
    summary="Ingest CSV files from S3 into Firestore",
    description=(
        "Reads all `.csv` files from the configured S3 bucket/prefix, "
        "parses transaction records, deduplicates them against the main Firestore "
        "collection, inserts new records, and returns a summary of the operation."
    ),
)
async def process_files(
    service: IngestionService = Depends(get_ingestion_service),
) -> IngestionResponse:
    """Trigger the full CSV ingestion pipeline.

    Args:
        service: Injected IngestionService dependency.

    Returns:
        IngestionResponse with counts of read, inserted, and discarded records.

    Raises:
        HTTPException: 503 if S3 or Firestore is unreachable.
    """
    try:
        result = await service.run()
        return result
    except S3ServiceError as exc:
        logger.error("S3 error during ingestion: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"S3 error: {exc}",
        ) from exc
    except FirestoreServiceError as exc:
        logger.error("Firestore error during ingestion: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Firestore error: {exc}",
        ) from exc
    except Exception as exc:
        logger.error("Unexpected error during ingestion: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred.",
        ) from exc
