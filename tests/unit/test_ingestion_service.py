"""Unit tests for the ingestion service module."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.models.transaction import Transaction
from app.services.ingestion_service import IngestionService


def _make_transaction(**kwargs) -> Transaction:
    defaults = {
        "value": 100.0,
        "date": "2024-01-15",
        "description": "Test transaction",
        "installment": "1/1",
        "source_file": "test.csv",
    }
    defaults.update(kwargs)
    return Transaction(**defaults)


class TestIngestionServiceRun:
    """Tests for IngestionService.run."""

    @pytest.mark.asyncio
    async def test_new_record_is_inserted(self):
        """Should insert a non-duplicate record into the main collection."""
        s3_service = MagicMock()
        firestore_service = MagicMock()

        # Provide one CSV file with one record
        s3_service.list_csv_files.return_value = ["prefix/test.csv"]
        s3_service.download_file.return_value = ("test.csv", b"amount|date|description|installments|category|bank|doc_type|owner|extraction_date\n100.0|2024-01-15|Test|1/1|cat|b|doc|own|date\n")
        firestore_service.bulk_insert_temp.return_value = None
        firestore_service.exists_in_main.return_value = False
        firestore_service.insert_into_main.return_value = True
        firestore_service.delete_all_temp.return_value = None

        service = IngestionService(s3_service=s3_service, firestore_service=firestore_service)
        result = await service.run()

        assert result.total_read == 1
        assert result.total_inserted == 1
        assert result.total_discarded == 0
        assert result.status == "success"

    @pytest.mark.asyncio
    async def test_duplicate_record_is_discarded(self):
        """Should discard a record that already exists in the main collection."""
        s3_service = MagicMock()
        firestore_service = MagicMock()

        s3_service.list_csv_files.return_value = ["prefix/test.csv"]
        s3_service.download_file.return_value = ("test.csv", b"amount|date|description|installments|category|bank|doc_type|owner|extraction_date\n100.0|2024-01-15|Test|1/1|cat|b|doc|own|date\n")
        firestore_service.bulk_insert_temp.return_value = None
        firestore_service.exists_in_main.return_value = True  # Duplicate!
        firestore_service.delete_all_temp.return_value = None

        service = IngestionService(s3_service=s3_service, firestore_service=firestore_service)
        result = await service.run()

        assert result.total_read == 1
        assert result.total_inserted == 0
        assert result.total_discarded == 1
        firestore_service.insert_into_main.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_csv_files_returns_zero_counts(self):
        """Should return all-zero counts when no CSV files are found in S3."""
        s3_service = MagicMock()
        firestore_service = MagicMock()

        s3_service.list_csv_files.return_value = []
        service = IngestionService(s3_service=s3_service, firestore_service=firestore_service)
        result = await service.run()

        assert result.total_read == 0
        assert result.total_inserted == 0
        assert result.total_discarded == 0
        firestore_service.bulk_insert_temp.assert_not_called()

    @pytest.mark.asyncio
    async def test_malformed_csv_is_skipped(self):
        """Should skip a malformed CSV file and continue processing others."""
        s3_service = MagicMock()
        firestore_service = MagicMock()

        s3_service.list_csv_files.return_value = ["bad.csv", "good.csv"]
        s3_service.download_file.side_effect = [
            ("bad.csv", b""),  # Empty = malformed
            ("good.csv", b"amount|date|description|installments|category|bank|doc_type|owner|extraction_date\n50.0|2024-01-15|desc|1/1|cat|b|doc|own|date\n"),
        ]
        firestore_service.bulk_insert_temp.return_value = None
        firestore_service.exists_in_main.return_value = False
        firestore_service.insert_into_main.return_value = True
        firestore_service.delete_all_temp.return_value = None

        service = IngestionService(s3_service=s3_service, firestore_service=firestore_service)
        result = await service.run()

        assert result.total_read == 1
        assert result.total_inserted == 1

    @pytest.mark.asyncio
    async def test_temp_collection_is_always_cleaned(self):
        """Should always delete temp collection even if inserts fail."""
        s3_service = MagicMock()
        firestore_service = MagicMock()

        s3_service.list_csv_files.return_value = ["f.csv"]
        s3_service.download_file.return_value = ("f.csv", b"amount|date|description|installments|category|bank|doc_type|owner|extraction_date\n10.0|2024-01-01|d|1/1|cat|b|doc|own|date\n")
        firestore_service.bulk_insert_temp.return_value = None
        firestore_service.exists_in_main.return_value = False
        firestore_service.insert_into_main.return_value = False  # Insert fails
        firestore_service.delete_all_temp.return_value = None

        service = IngestionService(s3_service=s3_service, firestore_service=firestore_service)
        await service.run()

        firestore_service.delete_all_temp.assert_called_once()
