"""CSV parsing helpers for financial transaction records."""

import csv
import io
import logging
from typing import List

from app.models.transaction import Transaction

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = {"amount", "date", "description", "installments", "category", "bank", "doc_type", "owner", "extraction_date"}


class CSVParseError(Exception):
    """Raised when a CSV file cannot be parsed due to structural issues."""


def parse_csv_content(content: bytes, source_file: str) -> List[Transaction]:
    """Parse raw CSV bytes into a list of Transaction records.

    Skips malformed rows and logs warnings for each skipped row.
    Raises CSVParseError if required columns are missing or the file is empty.

    Args:
        content: Raw bytes of the CSV file.
        source_file: Name of the source file (used for logging and record metadata).

    Returns:
        A list of parsed Transaction objects.

    Raises:
        CSVParseError: If the file is empty or missing required columns.
    """
    decoded = content.decode("utf-8-sig").strip()
    if not decoded:
        raise CSVParseError(f"File '{source_file}' is empty.")

    # Use pipe delimiter as specified by the user
    reader = csv.DictReader(io.StringIO(decoded), delimiter="|")

    # Validate headers
    if reader.fieldnames is None:
        raise CSVParseError(f"File '{source_file}' has no headers.")

    normalized_fieldnames = {col.strip().lower() for col in reader.fieldnames}
    missing = REQUIRED_COLUMNS - normalized_fieldnames
    if missing:
        raise CSVParseError(
            f"File '{source_file}' is missing required columns: {missing}"
        )

    transactions: List[Transaction] = []
    for i, row in enumerate(reader, start=2):  # row 1 is the header
        # Normalize keys
        normalized_row = {k.strip().lower(): v.strip() for k, v in row.items() if k}
        description = normalized_row.get("description", "")

        # Skip specific records requested by the user
        if "RECONCILIATION_DIFFERENCE" in description.upper():
            logger.debug("Skipping reconciliation record at row %d.", i)
            continue

        try:
            # Map 'amount' to 'value' and 'installments' to 'installment'
            value = _parse_float(normalized_row.get("amount", ""))
            
            # Transformations for new fields
            bank = normalized_row.get("bank", "").title()
            
            doc_type_raw = normalized_row.get("doc_type", "").lower()
            if doc_type_raw == "bank statement":
                doc_type = "conta corrente"
            elif doc_type_raw == "credit card statement":
                doc_type = "cartão de crédito"
            else:
                doc_type = normalized_row.get("doc_type", "")
                
            owner_raw = normalized_row.get("owner", "")
            owner = owner_raw.split()[0].title() if owner_raw else ""
            
            extraction_date = normalized_row.get("extraction_date", "")

            transaction = Transaction(
                value=value,
                date=normalized_row["date"],
                description=normalized_row["description"],
                installment=normalized_row["installments"],
                bank=bank,
                doc_type=doc_type,
                owner=owner,
                extraction_date=extraction_date,
                category=normalized_row.get("category"),
                source_file=source_file,
            )
            transactions.append(transaction)
            logger.debug(
                "Parsed row %d from '%s': value=%.2f date=%s",
                i,
                source_file,
                value,
                transaction.date,
            )
        except (ValueError, KeyError) as exc:
            logger.warning(
                "Skipping malformed row %d in '%s': %s", i, source_file, exc
            )

    logger.info("Parsed %d records from '%s'.", len(transactions), source_file)
    return transactions


def _parse_float(raw: str) -> float:
    """Convert a raw string to a float, handling common formatting issues.

    Args:
        raw: The raw string value (may contain currency symbols, commas).

    Returns:
        The parsed float value.

    Raises:
        ValueError: If the string cannot be converted to a float.
    """
    cleaned = raw.replace(",", ".").replace(" ", "").replace("R$", "").strip()
    if not cleaned:
        raise ValueError("Empty value field.")
    return float(cleaned)
