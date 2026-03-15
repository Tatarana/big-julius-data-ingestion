"""Firestore service for reading and writing transaction documents."""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Protocol

logger = logging.getLogger(__name__)


class FirestoreClientProtocol(Protocol):
    """Protocol for an injectable Firestore client (for testability)."""

    def collection(self, name: str) -> Any: ...


class FirestoreServiceError(Exception):
    """Raised when a Firestore operation fails in an unrecoverable way."""


class FirestoreService:
    """Provides typed Firestore operations for the ingestion flow.

    All methods are synchronous wrappers around the google-cloud-firestore
    client for compatibility with the current async FastAPI context (via
    run_in_executor when needed).

    Attributes:
        _client: The injectable Firestore client.
        _main_collection: Name of the main transactions collection.
        _temp_collection: Name of the temporary staging collection.
        _rules_collection: Name of the classification rules collection.
    """

    def __init__(
        self,
        client: FirestoreClientProtocol,
        main_collection: str,
        temp_collection: str,
        rules_collection: str = "classification_rules",
    ) -> None:
        """Initialize the FirestoreService.

        Args:
            client: An injectable Firestore client (or mock).
            main_collection: Name of the main Firestore collection.
            temp_collection: Name of the temporary Firestore collection.
            rules_collection: Name of the classification rules collection.
        """
        self._client = client
        self._main_collection = main_collection
        self._temp_collection = temp_collection
        self._rules_collection = rules_collection

    def bulk_insert_temp(self, records: List[Dict[str, Any]]) -> None:
        """Bulk-insert transaction records into the temporary collection.

        Uses Firestore batch writes for efficiency (max 500 per batch).

        Args:
            records: List of transaction dicts to insert.
        """
        batch_size = 500
        collection_ref = self._client.collection(self._temp_collection)
        now = datetime.now(tz=timezone.utc)

        for i in range(0, len(records), batch_size):
            batch = self._client.batch()
            chunk = records[i : i + batch_size]
            for record in chunk:
                doc_ref = collection_ref.document()
                batch.set(doc_ref, {**record, "ingested_at": now})
            try:
                batch.commit()
                logger.debug("Committed batch of %d docs to '%s'.", len(chunk), self._temp_collection)
            except Exception as exc:
                logger.error("Error committing batch to temp collection: %s", exc, exc_info=True)
                raise FirestoreServiceError("Firestore batch write failed.") from exc

    def exists_in_main(self, dedup_key: Dict[str, Any]) -> bool:
        """Check whether a matching record already exists in the main collection.

        Args:
            dedup_key: Dict with keys value, date, description, installment.

        Returns:
            True if a matching document exists, False otherwise.
        """
        query = self._client.collection(self._main_collection)
        for field, value in dedup_key.items():
            query = query.where(field, "==", value)
        docs = list(query.limit(1).stream())
        return len(docs) > 0

    def insert_into_main(self, record: Dict[str, Any]) -> bool:
        """Insert a single record into the main collection.

        Args:
            record: Transaction dict to insert.

        Returns:
            True if inserted successfully, False on error.
        """
        try:
            self._client.collection(self._main_collection).add(record)
            logger.debug("Inserted record into '%s': %s", self._main_collection, record.get("description"))
            return True
        except Exception as exc:
            logger.error(
                "Failed to insert record into '%s': %s",
                self._main_collection,
                exc,
                exc_info=True,
            )
            return False

    def delete_all_temp(self) -> None:
        """Delete all documents from the temporary collection.

        Deletes in batches of 500 until no documents remain.
        """
        collection_ref = self._client.collection(self._temp_collection)
        deleted_total = 0

        while True:
            docs = list(collection_ref.limit(500).stream())
            if not docs:
                break
            batch = self._client.batch()
            for doc in docs:
                batch.delete(doc.reference)
            batch.commit()
            deleted_total += len(docs)
            logger.debug("Deleted %d docs from '%s' (running total %d).", len(docs), self._temp_collection, deleted_total)

        logger.info("Cleaned up %d documents from '%s'.", deleted_total, self._temp_collection)

    def get_all_temp(self) -> List[Dict[str, Any]]:
        """Retrieve all documents from the temporary collection.

        Returns:
            A list of dicts representing temporary transaction documents,
            each including the document id under the key '_doc_id'.
        """
        docs = self._client.collection(self._temp_collection).stream()
        return [{"_doc_id": doc.id, **doc.to_dict()} for doc in docs]

    # --- Classification rules methods ---

    def get_all_rules(self) -> List[Dict[str, Any]]:
        """Retrieve all classification rules.

        Returns:
            A list of dicts with 'description' and 'manual_category' fields.
        """
        docs = self._client.collection(self._rules_collection).stream()
        return [{"_doc_id": doc.id, **doc.to_dict()} for doc in docs]

    def search_rules(
        self,
        description: Optional[str] = None,
        category: Optional[str] = None,
        subcategory: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Search for classification rules using 'contains' logic (case-insensitive).

        Args:
            description: Partial match for the description.
            category: Partial match for the manual_category.
            subcategory: Partial match for the manual_subcategory.

        Returns:
            A filter list of classification rules.
        """
        all_rules = self.get_all_rules()
        filtered = []

        desc_q = description.lower() if description else None
        cat_q = category.lower() if category else None
        sub_q = subcategory.lower() if subcategory else None

        for rule in all_rules:
            # Check description
            if desc_q and desc_q not in rule.get("description", "").lower():
                continue
            # Check category
            if cat_q and cat_q not in rule.get("manual_category", "").lower():
                continue
            # Check subcategory
            if sub_q and sub_q not in rule.get("manual_subcategory", "").lower():
                continue
            
            filtered.append(rule)

        return filtered

    def get_rule(self, doc_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a single classification rule by its ID.

        Args:
            doc_id: Firestore document ID.

        Returns:
            The rule data as a dict (with _doc_id), or None if not found.
        """
        doc = self._client.collection(self._rules_collection).document(doc_id).get()
        if not doc.exists:
            return None
        return {"_doc_id": doc.id, **doc.to_dict()}

    def add_rule(self, rule: Dict[str, Any]) -> str:
        """Insert a new classification rule.

        Args:
            rule: Dict with 'description' and 'manual_category'.

        Returns:
            The Firestore document ID of the newly created rule.
        """
        try:
            _, doc_ref = self._client.collection(self._rules_collection).add(rule)
            logger.info("Added classification rule: %s -> %s", rule.get("description"), rule.get("manual_category"))
            return doc_ref.id
        except Exception as exc:
            logger.error("Failed to add classification rule: %s", exc, exc_info=True)
            raise FirestoreServiceError("Failed to add classification rule.") from exc

    def update_rule(self, doc_id: str, updates: Dict[str, Any]) -> None:
        """Update a classification rule by document ID.

        Args:
            doc_id: Firestore document ID of the rule.
            updates: Dict of field names and their new values.

        Raises:
            FirestoreServiceError: If the update fails.
        """
        try:
            doc_ref = self._client.collection(self._rules_collection).document(doc_id)
            doc_ref.update(updates)
            logger.info("Updated classification rule '%s': %s", doc_id, updates)
        except Exception as exc:
            logger.error("Failed to update classification rule '%s': %s", doc_id, exc, exc_info=True)
            raise FirestoreServiceError(f"Failed to update classification rule {doc_id}.") from exc

    def delete_rule(self, doc_id: str) -> None:
        """Delete a classification rule by document ID.

        Args:
            doc_id: Firestore document ID of the rule.

        Raises:
            FirestoreServiceError: If the deletion fails.
        """
        try:
            doc_ref = self._client.collection(self._rules_collection).document(doc_id)
            doc_ref.delete()
            logger.info("Deleted classification rule '%s'.", doc_id)
        except Exception as exc:
            logger.error("Failed to delete classification rule '%s': %s", doc_id, exc, exc_info=True)
            raise FirestoreServiceError(f"Failed to delete classification rule {doc_id}.") from exc

    def get_pending_transactions(self) -> List[Dict[str, Any]]:
        """Retrieve all transactions with classification_review_status == 'pending'.

        Returns:
            A list of dicts including '_doc_id' for each pending transaction.
        """
        query = (
            self._client.collection(self._main_collection)
            .where("classification_review_status", "==", "pending")
        )
        docs = list(query.stream())
        return [{"_doc_id": doc.id, **doc.to_dict()} for doc in docs]

    def update_transaction(self, doc_id: str, updates: Dict[str, Any]) -> None:
        """Update specific fields on a transaction document in the main collection.

        Args:
            doc_id: Firestore document ID.
            updates: Dict of field names and their new values.
        """
        try:
            doc_ref = self._client.collection(self._main_collection).document(doc_id)
            doc_ref.update(updates)
            logger.debug("Updated transaction '%s': %s", doc_id, updates)
        except Exception as exc:
            logger.error("Failed to update transaction '%s': %s", doc_id, exc, exc_info=True)
            raise FirestoreServiceError(f"Failed to update transaction {doc_id}.") from exc


def build_firestore_client(
    project_id: str,
    database: str = "(default)",
    credentials_path: Optional[str] = None,
) -> FirestoreClientProtocol:
    """Build and return a production Firestore client.

    Args:
        project_id: Google Cloud project ID.
        database: Firestore database ID.
        credentials_path: Optional path to service account JSON (falls back to
            GOOGLE_APPLICATION_CREDENTIALS env var or ADC).

    Returns:
        A configured google-cloud-firestore Client.
    """
    from google.cloud import firestore  # type: ignore

    if credentials_path:
        from google.oauth2 import service_account  # type: ignore

        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        return firestore.Client(project=project_id, database=database, credentials=credentials)

    return firestore.Client(project=project_id, database=database)
