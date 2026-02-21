"""Pydantic models for transaction records."""

from datetime import datetime
from typing import Optional

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
    """

    value: float = Field(..., description="Monetary amount of the transaction.")
    date: str = Field(..., description="Transaction date in YYYY-MM-DD format.")
    description: str = Field(..., description="Description of the transaction.")
    installment: str = Field(..., description="Installment indicator, e.g. '1/4'.")
    category: Optional[str] = Field(None, description="Transaction category.")
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


class IngestionResponse(BaseModel):
    """Response model for the POST /process-files endpoint.

    Attributes:
        total_read: Total number of records parsed from all CSV files.
        total_inserted: Records successfully inserted into the main collection.
        total_discarded: Records skipped due to duplication.
        status: Overall status string, always "success" on HTTP 200.
    """

    total_read: int = Field(..., description="Total records parsed from all CSV files.")
    total_inserted: int = Field(..., description="Records inserted into the main collection.")
    total_discarded: int = Field(..., description="Records discarded due to duplication.")
    status: str = Field("success", description="Processing status.")
