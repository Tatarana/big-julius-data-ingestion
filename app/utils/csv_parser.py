"""CSV parsing helpers for financial transaction records."""

import csv
import io
import logging
from typing import List, Optional

from app.models.transaction import Transaction

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = {"amount", "date", "description", "installments", "category", "bank", "doc_type", "owner", "extraction_date", "payment_date"}


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
            
            extraction_date_raw = normalized_row.get("extraction_date", "")
            extraction_date = _normalize_date_to_ddmmyyyy(extraction_date_raw)

            payment_date_raw = normalized_row.get("payment_date", "")
            payment_date = _normalize_date_to_ddmmyyyy(payment_date_raw) if payment_date_raw else None

            category = normalized_row.get("category")
            classification_review_status = (
                "pending" if category and category.strip().lower() == "outros" else None
            )

            settlement_period = _calculate_settlement_period(
                normalized_row["date"],
                normalized_row["installments"],
                doc_type,
            )

            transaction = Transaction(
                value=value,
                date=normalized_row["date"],
                description=normalized_row["description"],
                installment=normalized_row["installments"],
                bank=bank,
                doc_type=doc_type,
                owner=owner,
                extraction_date=extraction_date,
                payment_date=payment_date,
                settlement_period=settlement_period,
                category=category,
                classification_review_status=classification_review_status,
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


def _calculate_settlement_period(
    date_str: str, installment: str, doc_type: str
) -> Optional[str]:
    """Calculate the settlement period (MM-YYYY) for a transaction.

    For credit card transactions with installments, the settlement period is
    the purchase month plus (current_installment - 1) months.
    For all other transactions, it is simply the month/year of the date.

    Args:
        date_str: Transaction date in YYYY-MM-DD or DD-MM-YYYY format.
        installment: Installment string, e.g. '3/5'.
        doc_type: Document type (already transformed, e.g. 'cartão de crédito').

    Returns:
        Settlement period as 'MM-YYYY', or None if the date cannot be parsed.
    """
    try:
        parts = date_str.strip().split("-")
        if len(parts[0]) == 4:
            # YYYY-MM-DD
            year = int(parts[0])
            month = int(parts[1])
        else:
            # DD-MM-YYYY
            year = int(parts[2])
            month = int(parts[1])
    except (IndexError, ValueError):
        logger.warning("Cannot parse date '%s' for settlement_period calculation.", date_str)
        return None

    months_to_add = 0
    if doc_type == "cartão de crédito" and installment:
        try:
            current, _ = installment.split("/")
            months_to_add = int(current) - 1
        except (ValueError, AttributeError):
            logger.warning(
                "Cannot parse installment '%s' for settlement_period calculation.", installment
            )

    total_months = (year * 12 + month - 1) + months_to_add
    result_year = total_months // 12
    result_month = (total_months % 12) + 1

    return f"{result_month:02d}-{result_year}"


def _normalize_date_to_ddmmyyyy(date_str: str) -> str:
    """Convert a date string to DD-MM-YYYY format.

    If already in DD-MM-YYYY, returns as-is. If in YYYY-MM-DD, converts it.

    Args:
        date_str: Date string in YYYY-MM-DD or DD-MM-YYYY format.

    Returns:
        Date string in DD-MM-YYYY format, or the original string if unparseable.
    """
    stripped = date_str.strip()
    if not stripped:
        return stripped

    parts = stripped.split("-")
    if len(parts) == 3 and len(parts[0]) == 4:
        # YYYY-MM-DD → DD-MM-YYYY
        return f"{parts[2]}-{parts[1]}-{parts[0]}"

    return stripped
