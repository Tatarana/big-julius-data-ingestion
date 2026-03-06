"""Unit tests for the reclassification logic in IngestionService."""

from unittest.mock import MagicMock

import pytest

from app.services.ingestion_service import IngestionService


def _make_service():
    """Create an IngestionService with mocked dependencies."""
    s3_service = MagicMock()
    firestore_service = MagicMock()
    return IngestionService(s3_service=s3_service, firestore_service=firestore_service)


class TestReclassifyPending:
    """Tests for IngestionService.reclassify_pending."""

    def test_no_pending_transactions_returns_zero(self):
        """Should return 0 when there are no pending transactions."""
        service = _make_service()
        service._firestore.get_all_rules.return_value = [
            {"_doc_id": "r1", "description": "supermarket", "manual_category": "Food"},
        ]
        service._firestore.get_pending_transactions.return_value = []

        result = service.reclassify_pending()

        assert result == 0
        service._firestore.update_transaction.assert_not_called()

    def test_no_rules_marks_all_as_reviewed(self):
        """Should mark all pending as reviewed when no rules exist."""
        service = _make_service()
        service._firestore.get_all_rules.return_value = []
        service._firestore.get_pending_transactions.return_value = [
            {"_doc_id": "t1", "description": "Supermarket purchase", "category": "outros"},
        ]

        result = service.reclassify_pending()

        assert result == 0
        service._firestore.update_transaction.assert_called_once_with("t1", {
            "classification_review_status": "reviewed",
        })

    def test_matching_rule_reclassifies_transaction(self):
        """Should update category when a rule matches the transaction description."""
        service = _make_service()
        service._firestore.get_all_rules.return_value = [
            {"_doc_id": "r1", "description": "supermarket", "manual_category": "Food"},
        ]
        service._firestore.get_pending_transactions.return_value = [
            {"_doc_id": "t1", "description": "Supermarket purchase today", "category": "outros"},
        ]

        result = service.reclassify_pending()

        assert result == 1
        service._firestore.update_transaction.assert_called_once_with("t1", {
            "category": "Food",
            "classification_review_status": "reviewed",
        })

    def test_no_matching_rule_marks_as_reviewed(self):
        """Should mark as reviewed without changing category when no rule matches."""
        service = _make_service()
        service._firestore.get_all_rules.return_value = [
            {"_doc_id": "r1", "description": "pharmacy", "manual_category": "Health"},
        ]
        service._firestore.get_pending_transactions.return_value = [
            {"_doc_id": "t1", "description": "Supermarket purchase", "category": "outros"},
        ]

        result = service.reclassify_pending()

        assert result == 0
        service._firestore.update_transaction.assert_called_once_with("t1", {
            "classification_review_status": "reviewed",
        })

    def test_longest_match_wins(self):
        """Should prefer the longest matching rule description."""
        service = _make_service()
        service._firestore.get_all_rules.return_value = [
            {"_doc_id": "r1", "description": "market", "manual_category": "Shopping"},
            {"_doc_id": "r2", "description": "supermarket", "manual_category": "Food"},
        ]
        service._firestore.get_pending_transactions.return_value = [
            {"_doc_id": "t1", "description": "Supermarket purchase", "category": "outros"},
        ]

        result = service.reclassify_pending()

        assert result == 1
        service._firestore.update_transaction.assert_called_once_with("t1", {
            "category": "Food",
            "classification_review_status": "reviewed",
        })

    def test_multiple_pending_transactions(self):
        """Should process multiple pending transactions correctly."""
        service = _make_service()
        service._firestore.get_all_rules.return_value = [
            {"_doc_id": "r1", "description": "gas station", "manual_category": "Transport"},
        ]
        service._firestore.get_pending_transactions.return_value = [
            {"_doc_id": "t1", "description": "Gas station refuel", "category": "outros"},
            {"_doc_id": "t2", "description": "Random purchase", "category": "outros"},
        ]

        result = service.reclassify_pending()

        assert result == 1  # Only t1 matched
        assert service._firestore.update_transaction.call_count == 2

    def test_case_insensitive_matching(self):
        """Should match rules case-insensitively."""
        service = _make_service()
        service._firestore.get_all_rules.return_value = [
            {"_doc_id": "r1", "description": "UBER", "manual_category": "Transport"},
        ]
        service._firestore.get_pending_transactions.return_value = [
            {"_doc_id": "t1", "description": "Uber ride downtown", "category": "outros"},
        ]

        result = service.reclassify_pending()

        assert result == 1
        service._firestore.update_transaction.assert_called_once_with("t1", {
            "category": "Transport",
            "classification_review_status": "reviewed",
        })
