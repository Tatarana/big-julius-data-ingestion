"""Pydantic models for transaction records."""

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class Transaction(BaseModel):
    """Represents a financial transaction record parsed from a CSV file.

    Attributes:
        value: Monetary amount of the transaction (positive or negative float).
        date: Transaction date in YYYY-MM-DD format.
        description: Human-readable description of the transaction.
        installment: Installment indicator string, e.g. "1/4".
        source_file: Name of the source CSV file this record came from.
        ingested_at: UTC timestamp when the record was ingested.
        classification_review_status: Review status for category fine-tuning.
            "pending" for "outros" transactions, None otherwise.
    """

    value: float = Field(..., description="Monetary amount of the transaction.")
    date: str = Field(..., description="Transaction date in YYYY-MM-DD format.")
    description: str = Field(..., description="Description of the transaction.")
    installment: str = Field(..., description="Installment indicator, e.g. '1/4'.")
    bank: str = Field(..., description="Bank name.")
    doc_type: str = Field(..., description="Document type.")
    owner: str = Field(..., description="Owner's first name.")
    extraction_date: str = Field(..., description="Date the data was extracted.")
    settlement_period: Optional[str] = Field(
        None,
        description="Settlement period in MM-YYYY format. Calculated from date and installment for credit card transactions.",
    )
    category: Optional[str] = Field(None, description="Transaction category.")
    classification_review_status: Optional[str] = Field(
        None,
        description="Review status: 'pending' for outros, 'reviewed' after check, None otherwise.",
    )
    source_file: Optional[str] = Field(None, description="Source CSV filename.")
    ingested_at: Optional[datetime] = Field(None, description="UTC ingestion timestamp.")

    def dedup_key(self) -> dict:
        """Return the fields used for deduplication comparison.

        Returns:
            A dict containing value, date, description, and installment.
        """
        return {
            "value": self.value,
            "date": self.date,
            "description": self.description,
            "installment": self.installment,
        }


class ClassificationRule(BaseModel):
    """A manual classification rule for category fine-tuning.

    Attributes:
        description: Text pattern to match against transaction descriptions.
        manual_category: The category to assign when the pattern matches.
    """

    description: str = Field(..., description="Text pattern to match in transaction descriptions.")
    manual_category: str = Field(..., description="Category to assign on match.")


class ClassificationRuleResponse(BaseModel):
    """Response model for classification rule endpoints.

    Attributes:
        id: Firestore document ID.
        description: Text pattern to match against transaction descriptions.
        manual_category: The category to assign when the pattern matches.
    """

    id: str = Field(..., description="Firestore document ID.")
    description: str = Field(..., description="Text pattern to match in transaction descriptions.")
    manual_category: str = Field(..., description="Category to assign on match.")


class IngestionResponse(BaseModel):
    """Response model for the POST /process-files endpoint.

    Attributes:
        total_read: Total number of records parsed from all CSV files.
        total_inserted: Records successfully inserted into the main collection.
        total_discarded: Records skipped due to duplication.
        total_reclassified: Records reclassified by classification rules.
        status: Overall status string, always "success" on HTTP 200.
    """

    total_read: int = Field(..., description="Total records parsed from all CSV files.")
    total_inserted: int = Field(..., description="Records inserted into the main collection.")
    total_discarded: int = Field(..., description="Records discarded due to duplication.")
    total_reclassified: int = Field(0, description="Records reclassified by classification rules.")
    status: str = Field("success", description="Processing status.")
