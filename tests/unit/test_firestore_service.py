"""Unit tests for the Firestore service module."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, call, patch

import pytest

from app.services.firestore_service import FirestoreService, FirestoreServiceError


def _make_service(main_col="transactions", temp_col="transactions_temp"):
    client = MagicMock()
    return FirestoreService(client=client, main_collection=main_col, temp_collection=temp_col)


class TestBulkInsertTemp:
    """Tests for FirestoreService.bulk_insert_temp."""

    def test_inserts_records_in_batches(self):
        """Should write records to the temp collection using batched writes."""
        service = _make_service()
        records = [{"value": 10.0, "date": "2024-01-01", "description": "d", "installment": "1/1"}]
        service.bulk_insert_temp(records)
        service._client.collection.assert_called_with("transactions_temp")
        service._client.batch().commit.assert_called_once()

    def test_raises_on_commit_failure(self):
        """Should raise FirestoreServiceError when batch commit fails."""
        service = _make_service()
        service._client.batch().commit.side_effect = Exception("Firestore down")
        with pytest.raises(FirestoreServiceError):
            service.bulk_insert_temp([{"value": 1.0, "date": "2024-01-01", "description": "d", "installment": "1/1"}])


class TestExistsInMain:
    """Tests for FirestoreService.exists_in_main."""

    def test_returns_true_when_doc_found(self):
        """Should return True when a matching doc is found in main collection."""
        service = _make_service()
        mock_doc = MagicMock()
        service._client.collection().where().where().where().where().limit().stream.return_value = iter([mock_doc])
        # Simplified: mock the full chain
        collection_mock = MagicMock()
        collection_mock.where.return_value = collection_mock
        collection_mock.limit.return_value = collection_mock
        collection_mock.stream.return_value = iter([mock_doc])
        service._client.collection.return_value = collection_mock

        result = service.exists_in_main({"value": 10.0, "date": "2024-01-01", "description": "d", "installment": "1/1"})
        assert result is True

    def test_returns_false_when_no_doc_found(self):
        """Should return False when no matching doc is found in main collection."""
        service = _make_service()
        collection_mock = MagicMock()
        collection_mock.where.return_value = collection_mock
        collection_mock.limit.return_value = collection_mock
        collection_mock.stream.return_value = iter([])
        service._client.collection.return_value = collection_mock

        result = service.exists_in_main({"value": 10.0, "date": "2024-01-01", "description": "d", "installment": "1/1"})
        assert result is False


class TestInsertIntoMain:
    """Tests for FirestoreService.insert_into_main."""

    def test_returns_true_on_success(self):
        """Should return True when the record is inserted successfully."""
        service = _make_service()
        result = service.insert_into_main({"value": 10.0, "date": "2024-01-01", "description": "d", "installment": "1/1"})
        assert result is True
        service._client.collection().add.assert_called_once()

    def test_returns_false_on_failure(self):
        """Should return False when the Firestore add call raises an exception."""
        service = _make_service()
        service._client.collection().add.side_effect = Exception("write error")
        result = service.insert_into_main({"value": 10.0})
        assert result is False


class TestDeleteAllTemp:
    """Tests for FirestoreService.delete_all_temp."""

    def test_deletes_all_documents(self):
        """Should delete all documents from the temp collection in batches."""
        service = _make_service()
        mock_doc = MagicMock()
        mock_doc.reference = MagicMock()
        # First call returns docs, second returns empty (stop condition)
        collection_mock = MagicMock()
        collection_mock.limit.return_value.stream.side_effect = [
            [mock_doc, mock_doc],
            [],
        ]
        service._client.collection.return_value = collection_mock
        service.delete_all_temp()
        service._client.batch().commit.assert_called()

    def test_no_documents_does_not_error(self):
        """Should handle an empty temp collection gracefully."""
        service = _make_service()
        collection_mock = MagicMock()
        collection_mock.limit.return_value.stream.return_value = iter([])
        service._client.collection.return_value = collection_mock
        service.delete_all_temp()  # Should not raise


class TestGetAllRules:
    """Tests for FirestoreService.get_all_rules."""

    def test_returns_rules_with_doc_ids(self):
        """Should return all rules with _doc_id included."""
        service = _make_service()
        mock_doc = MagicMock()
        mock_doc.id = "rule1"
        mock_doc.to_dict.return_value = {"description": "uber", "manual_category": "Transport"}
        service._client.collection.return_value.stream.return_value = iter([mock_doc])

        rules = service.get_all_rules()

        assert len(rules) == 1
        assert rules[0]["_doc_id"] == "rule1"
        assert rules[0]["description"] == "uber"
        assert rules[0]["manual_category"] == "Transport"

    def test_returns_empty_list_when_no_rules(self):
        """Should return an empty list when no rules exist."""
        service = _make_service()
        service._client.collection.return_value.stream.return_value = iter([])
        rules = service.get_all_rules()
        assert rules == []


class TestAddRule:
    """Tests for FirestoreService.add_rule."""

    def test_adds_rule_and_returns_id(self):
        """Should add a rule and return the new document ID."""
        service = _make_service()
        mock_doc_ref = MagicMock()
        mock_doc_ref.id = "new_rule_id"
        service._client.collection.return_value.add.return_value = (None, mock_doc_ref)

        rule = {"description": "uber", "manual_category": "Transport"}
        result = service.add_rule(rule)

        assert result == "new_rule_id"
        service._client.collection.return_value.add.assert_called_once_with(rule)

    def test_raises_on_failure(self):
        """Should raise FirestoreServiceError when add fails."""
        service = _make_service()
        service._client.collection.return_value.add.side_effect = Exception("write error")

        with pytest.raises(FirestoreServiceError):
            service.add_rule({"description": "test", "manual_category": "cat"})


class TestGetPendingTransactions:
    """Tests for FirestoreService.get_pending_transactions."""

    def test_returns_pending_transactions(self):
        """Should return transactions with pending status."""
        service = _make_service()
        mock_doc = MagicMock()
        mock_doc.id = "txn1"
        mock_doc.to_dict.return_value = {
            "description": "test",
            "category": "outros",
            "classification_review_status": "pending",
        }

        collection_mock = MagicMock()
        collection_mock.where.return_value = collection_mock
        collection_mock.stream.return_value = [mock_doc]
        service._client.collection.return_value = collection_mock

        result = service.get_pending_transactions()

        assert len(result) == 1
        assert result[0]["_doc_id"] == "txn1"
        assert result[0]["classification_review_status"] == "pending"


class TestUpdateTransaction:
    """Tests for FirestoreService.update_transaction."""

    def test_updates_document_fields(self):
        """Should call update on the correct document reference."""
        service = _make_service()
        updates = {"category": "Food", "classification_review_status": "reviewed"}
        service.update_transaction("doc123", updates)

        service._client.collection.return_value.document.assert_called_once_with("doc123")
        service._client.collection.return_value.document.return_value.update.assert_called_once_with(updates)

    def test_raises_on_failure(self):
        """Should raise FirestoreServiceError when update fails."""
        service = _make_service()
        service._client.collection.return_value.document.return_value.update.side_effect = Exception("update error")

        with pytest.raises(FirestoreServiceError):
            service.update_transaction("doc123", {"category": "Food"})

