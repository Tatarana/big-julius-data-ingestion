"""Unit tests for the CSV parser module."""

import pytest

from app.utils.csv_parser import CSVParseError, parse_csv_content


VALID_CSV = b"amount|date|description|installments|category|bank|doc_type|owner|extraction_date\n100.50|2024-01-15|Supermarket|1/1|Food|nubank|bank statement|FERNANDO SILVA|2024-01-16\n-50.00|2024-01-16|Gas station|2/4|Transport|itau|credit card statement|joao pessoa|2024-01-17\n"
EMPTY_CSV = b""
WHITESPACE_CSV = b"   "
MISSING_COLUMNS_CSV = b"amount|date\n100.50|2024-01-15\n"
MALFORMED_ROW_CSV = b"amount|date|description|installments|category|bank|doc_type|owner|extraction_date\nnot_a_number|2024-01-15|desc|1/1|cat|b|doc|own|date\n150.00|2024-01-16|valid desc|1/2|cat|b|doc|own|date\n"
EMPTY_ROWS_CSV = b"amount|date|description|installments|category|bank|doc_type|owner|extraction_date\n"
BOM_CSV = b"\xef\xbb\xbfamount|date|description|installments|category|bank|doc_type|owner|extraction_date\n75.00|2024-01-20|Coffee|1/1|Food|b|doc|own|date\n"
COMMA_DECIMAL_CSV = b"amount|date|description|installments|category|bank|doc_type|owner|extraction_date\n1.234,56|2024-01-10|desc|1/1|cat|b|doc|own|date\n"


class TestParseCsvContent:
    """Tests for parse_csv_content function."""

    def test_valid_csv_parses_correctly(self):
        """Should parse a well-formed CSV into a list of Transaction objects."""
        records = parse_csv_content(VALID_CSV, "test.csv")
        assert len(records) == 2
        assert records[0].value == 100.50
        assert records[0].date == "2024-01-15"
        assert records[0].description == "Supermarket"
        assert records[0].installment == "1/1"
        assert records[0].category == "Food"
        assert records[0].bank == "Nubank"
        assert records[0].doc_type == "conta corrente"
        assert records[0].owner == "Fernando"
        assert records[0].extraction_date == "2024-01-16"
        assert records[0].source_file == "test.csv"
        
        assert records[1].category == "Transport"
        assert records[1].value == -50.00
        assert records[1].bank == "Itau"
        assert records[1].doc_type == "cartão de crédito"
        assert records[1].owner == "Joao"
        assert records[1].extraction_date == "2024-01-17"

    def test_empty_file_raises_error(self):
        """Should raise CSVParseError for an empty file."""
        with pytest.raises(CSVParseError, match="empty"):
            parse_csv_content(EMPTY_CSV, "empty.csv")

    def test_whitespace_only_raises_error(self):
        """Should raise CSVParseError for a whitespace-only file."""
        with pytest.raises(CSVParseError):
            parse_csv_content(WHITESPACE_CSV, "whitespace.csv")

    def test_missing_columns_raises_error(self):
        """Should raise CSVParseError if required columns are missing."""
        with pytest.raises(CSVParseError, match="missing required columns"):
            parse_csv_content(MISSING_COLUMNS_CSV, "test.csv")

    def test_reconciliation_difference_is_filtered(self):
        """Should ignore rows with description containing 'RECONCILIATION_DIFFERENCE'."""
        csv_data = (
            b"amount|date|description|installments|category|bank|doc_type|owner|extraction_date\n"
            b"10.0|2024-01-01|Valid record|1/1|cat|b|doc|own|date\n"
            b'20.0|2024-01-01|"||RECONCILIATION_DIFFERENCE|0.9||"|1/1|cat|b|doc|own|date\n'
        )

        records = parse_csv_content(csv_data, "test.csv")
        assert len(records) == 1
        assert records[0].description == "Valid record"

    def test_malformed_row_is_skipped(self):
        """Should skip rows with invalid data and continue parsing."""
        records = parse_csv_content(MALFORMED_ROW_CSV, "malformed.csv")
        assert len(records) == 1
        assert records[0].value == 150.00

    def test_all_malformed_rows_returns_empty_list(self):
        """Should return empty list when all data rows are malformed."""
        records = parse_csv_content(EMPTY_ROWS_CSV, "no_data.csv")
        assert records == []

    def test_bom_utf8_is_handled(self):
        """Should correctly parse CSV files with UTF-8 BOM."""
        records = parse_csv_content(BOM_CSV, "bom.csv")
        assert len(records) == 1
        assert records[0].value == 75.00
