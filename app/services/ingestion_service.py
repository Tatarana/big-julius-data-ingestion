"""Core ingestion business logic orchestrating S3, CSV parsing, and Firestore."""

import logging
from typing import List

from app.models.transaction import IngestionResponse, Transaction
from app.services.firestore_service import FirestoreService
from app.services.s3_service import S3Service, S3ServiceError
from app.utils.csv_parser import CSVParseError, parse_csv_content

logger = logging.getLogger(__name__)


class IngestionService:
    """Orchestrates the full ingestion pipeline from S3 to Firestore.

    Attributes:
        _s3: S3Service used to list and download CSV files.
        _firestore: FirestoreService used for all Firestore operations.
    """

    def __init__(self, s3_service: S3Service, firestore_service: FirestoreService) -> None:
        """Initialize IngestionService with injected dependencies.

        Args:
            s3_service: An S3Service instance.
            firestore_service: A FirestoreService instance.
        """
        self._s3 = s3_service
        self._firestore = firestore_service

    async def run(self) -> IngestionResponse:
        """Execute the full ingestion pipeline and return a summary response.

        Steps:
            1. Read CSV files from S3.
            2. Write all parsed records to the temporary Firestore collection (B).
            3. Deduplicate against the main collection (A) and insert new records.
            4. Clean up the temporary collection (B).
            5. Return the ingestion summary.

        Returns:
            An IngestionResponse with counts of read, inserted, and discarded records.

        Raises:
            S3ServiceError: If the S3 bucket or prefix is inaccessible.
        """
        # Step 1: Read CSVs from S3
        all_transactions: List[Transaction] = await self._fetch_all_transactions()
        total_read = len(all_transactions)
        logger.info("Total records parsed from all CSV files: %d.", total_read)

        if total_read == 0:
            logger.info("No records to process. Skipping Firestore operations.")
            return IngestionResponse(
                total_read=0,
                total_inserted=0,
                total_discarded=0,
                status="success",
            )

        # Step 2: Bulk-insert into temp collection (B)
        records_as_dicts = [t.model_dump(exclude={"ingested_at"}) for t in all_transactions]
        self._firestore.bulk_insert_temp(records_as_dicts)
        logger.info("Inserted %d records into the temp collection.", total_read)

        # Step 3: Deduplication
        total_inserted, total_discarded = self._deduplicate_and_insert(all_transactions)

        # Step 4: Cleanup temp collection
        self._firestore.delete_all_temp()

        # Step 5: Return response
        logger.info(
            "Ingestion complete — read: %d, inserted: %d, discarded: %d.",
            total_read,
            total_inserted,
            total_discarded,
        )
        return IngestionResponse(
            total_read=total_read,
            total_inserted=total_inserted,
            total_discarded=total_discarded,
            status="success",
        )

    async def _fetch_all_transactions(self) -> List[Transaction]:
        """List and parse all CSV files from S3.

        Returns:
            A combined list of all Transaction objects across all CSV files.
        """
        csv_keys = self._s3.list_csv_files()
        all_records: List[Transaction] = []

        for key in csv_keys:
            try:
                filename, content = self._s3.download_file(key)
            except S3ServiceError as exc:
                logger.error("Skipping file '%s' due to S3 error: %s", key, exc)
                continue

            try:
                records = parse_csv_content(content, filename)
                all_records.extend(records)
            except CSVParseError as exc:
                logger.warning("Skipping malformed/empty file '%s': %s", key, exc)

        return all_records

    def _deduplicate_and_insert(self, transactions: List[Transaction]) -> tuple[int, int]:
        """Check each transaction against the main collection and insert if new.

        Args:
            transactions: List of Transaction objects to process.

        Returns:
            A tuple of (inserted_count, discarded_count).
        """
        inserted = 0
        discarded = 0

        for transaction in transactions:
            dedup_key = transaction.dedup_key()
            if self._firestore.exists_in_main(dedup_key):
                logger.debug("Duplicate found, discarding: %s", dedup_key)
                discarded += 1
            else:
                success = self._firestore.insert_into_main(transaction.model_dump(exclude={"ingested_at"}))
                if success:
                    inserted += 1
                else:
                    logger.error("Failed to insert transaction: %s", dedup_key)

        return inserted, discarded
