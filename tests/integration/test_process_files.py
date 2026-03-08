"""Integration tests for the POST /process-files endpoint."""

import io
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.routers.ingestion import get_ingestion_service


VALID_CSV_CONTENT = b"amount|date|description|installments|category|bank|doc_type|owner|extraction_date|payment_date\n100.0|2024-01-15|Supermarket|1/1|Food|nubank|bank statement|FERNANDO SILVA|2024-01-16|2024-01-15\n-50.0|2024-01-16|Gas|2/4|Transport|itau|credit card statement|joao pessoa|2024-01-17|2024-02-10\n"
DUPLICATE_CSV_CONTENT = b"amount|date|description|installments|category|bank|doc_type|owner|extraction_date|payment_date\n100.0|2024-01-15|Supermarket|1/1|Food|nubank|bank statement|FERNANDO SILVA|2024-01-16|2024-01-15\n"
EMPTY_CSV_CONTENT = b""


def _build_mock_service(
    csv_keys=None,
    csv_content=VALID_CSV_CONTENT,
    exists_in_main=False,
    insert_success=True,
):
    """Helper to build a mock IngestionService with controllable behavior."""
    from app.services.ingestion_service import IngestionService
    from app.services.s3_service import S3Service
    from app.services.firestore_service import FirestoreService

    s3_mock = MagicMock(spec=S3Service)
    firestore_mock = MagicMock(spec=FirestoreService)

    if csv_keys is None:
        csv_keys = ["prefix/test.csv"]

    s3_mock.list_csv_files.return_value = csv_keys
    if csv_keys:
        s3_mock.download_file.return_value = ("test.csv", csv_content)

    firestore_mock.bulk_insert_temp.return_value = None
    firestore_mock.exists_in_main.return_value = exists_in_main
    firestore_mock.insert_into_main.return_value = insert_success
    firestore_mock.delete_all_temp.return_value = None

    return IngestionService(s3_service=s3_mock, firestore_service=firestore_mock)


class TestProcessFilesEndpoint:
    """Integration tests for POST /process-files."""

    def test_success_with_new_records(self):
        """Should return correct counts when all records are new."""
        mock_service = _build_mock_service(exists_in_main=False)
        app.dependency_overrides[get_ingestion_service] = lambda: mock_service

        client = TestClient(app)
        response = client.post("/process-files")

        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "success"
        assert body["total_read"] == 2
        assert body["total_inserted"] == 2
        assert body["total_discarded"] == 0

        app.dependency_overrides.clear()

    def test_success_with_all_duplicates(self):
        """Should return correct counts when all records are duplicates."""
        mock_service = _build_mock_service(
            csv_content=DUPLICATE_CSV_CONTENT,
            exists_in_main=True,
        )
        app.dependency_overrides[get_ingestion_service] = lambda: mock_service

        client = TestClient(app)
        response = client.post("/process-files")

        assert response.status_code == 200
        body = response.json()
        assert body["total_read"] == 1
        assert body["total_inserted"] == 0
        assert body["total_discarded"] == 1

        app.dependency_overrides.clear()

    def test_success_with_no_files(self):
        """Should return zeros when no CSV files are found."""
        mock_service = _build_mock_service(csv_keys=[])
        app.dependency_overrides[get_ingestion_service] = lambda: mock_service

        client = TestClient(app)
        response = client.post("/process-files")

        assert response.status_code == 200
        body = response.json()
        assert body["total_read"] == 0
        assert body["total_inserted"] == 0
        assert body["total_discarded"] == 0

        app.dependency_overrides.clear()

    def test_s3_error_returns_503(self):
        """Should return 503 when S3 is unreachable."""
        from app.services.s3_service import S3ServiceError
        from app.services.ingestion_service import IngestionService

        mock_service = MagicMock(spec=IngestionService)
        mock_service.run.side_effect = S3ServiceError("Bucket not found")
        app.dependency_overrides[get_ingestion_service] = lambda: mock_service

        client = TestClient(app)
        response = client.post("/process-files")

        assert response.status_code == 503
        app.dependency_overrides.clear()

    def test_response_schema_is_correct(self):
        """Should return a JSON body matching the IngestionResponse schema."""
        mock_service = _build_mock_service()
        app.dependency_overrides[get_ingestion_service] = lambda: mock_service

        client = TestClient(app)
        response = client.post("/process-files")

        body = response.json()
        assert "total_read" in body
        assert "total_inserted" in body
        assert "total_discarded" in body
        assert "status" in body

        app.dependency_overrides.clear()

    def test_health_endpoint_returns_ok(self):
        """Should return 200 with status ok from the health endpoint."""
        client = TestClient(app)
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}

    def test_malformed_csv_does_not_return_500(self):
        """Should not return 500 when a CSV file is malformed."""
        mock_service = _build_mock_service(csv_content=EMPTY_CSV_CONTENT)
        app.dependency_overrides[get_ingestion_service] = lambda: mock_service

        client = TestClient(app)
        response = client.post("/process-files")

        assert response.status_code == 200
        app.dependency_overrides.clear()
