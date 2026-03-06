# big-julius-data-ingestion

A production-ready REST microservice for ingesting financial transaction records from CSV files stored in AWS S3 into Google Cloud Firestore, with built-in deduplication logic.

---

## Project Description

`big-julius-data-ingestion` exposes a single endpoint — `POST /process-files` — that:

1. Reads all `.csv` files from a configured S3 bucket/prefix.
2. Parses transaction records (value, date, description, installment).
3. Writes records to a temporary Firestore staging collection.
4. Deduplicates them against the main collection and inserts only new records.
5. Cleans up the temporary collection.
6. Returns a JSON summary of the operation.

---

## Prerequisites

| Requirement | Version |
|---|---|
| Python | 3.11+ |
| AWS credentials | IAM user/role with S3 read access |
| GCP credentials | Service account with Firestore read/write |
| Docker (optional) | 24+ |
| Poetry (optional) | 1.8+ |

---

## Running Locally (without Docker)

### 1. Install dependencies

```bash
# Using Poetry
poetry install

# OR using pip
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env with your real values
```

### 3. Start the server

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```
or

```bash
.venv\Scripts\uvicorn app.main:app --reload --port 8000
```

The API will be available at `http://localhost:8000`.  
Swagger UI: `http://localhost:8000/docs`

---

## Running with Docker

### 1. Configure environment

```bash
cp .env.example .env
# Edit .env — set GOOGLE_APPLICATION_CREDENTIALS to the absolute host path of the JSON key
```

### 2. Build and run

```bash
docker compose up --build
```

The service will be available at `http://localhost:8000`.

---

## Environment Variables Reference

| Variable | Description | Default |
|---|---|---|
| `AWS_ACCESS_KEY_ID` | AWS access key ID | _required_ |
| `AWS_SECRET_ACCESS_KEY` | AWS secret access key | _required_ |
| `AWS_REGION` | AWS region (e.g. `us-east-1`) | _required_ |
| `S3_BUCKET_NAME` | S3 bucket that contains the CSV files | _required_ |
| `S3_PREFIX` | Folder prefix inside the bucket (e.g. `transactions/2024/`) | _required_ |
| `GOOGLE_APPLICATION_CREDENTIALS` | Absolute path to GCP service account JSON | _required_ |
| `FIRESTORE_PROJECT_ID` | GCP project ID | _required_ |
| `COLLECTION_MAIN` | Main Firestore collection name | `transactions` |
| `COLLECTION_TEMP` | Temporary Firestore collection name | `transactions_temp` |
| `LOG_LEVEL` | Logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) | `INFO` |

---

## API Endpoint Documentation

### `POST /process-files`

Triggers the full ingestion pipeline.

**Request:** No body required.

**Response (200 OK):**

```json
{
  "total_read": 150,
  "total_inserted": 142,
  "total_discarded": 8,
  "status": "success"
}
```

**Error responses:**

| Status | Cause |
|---|---|
| `503 Service Unavailable` | S3 or Firestore is unreachable |
| `500 Internal Server Error` | Unexpected error |

---

### `GET /health`

Returns service health status (used by Docker healthcheck).

**Response (200 OK):**

```json
{ "status": "ok" }
```

---

## CSV File Format

Each CSV file must contain the following columns (case-insensitive):

| Column | Type | Example |
|---|---|---|
| `amount` | float | `100.50` or `-25.00` |
| `date` | string (YYYY-MM-DD) | `2024-01-15` |
| `description` | string | `Supermarket` |
| `installments` | string | `1/4` |
| `bank` | string | `Nubank` |
| `doc_type` | string | `conta corrente` |
| `owner` | string | `Fernando` |
| `extraction_date` | string (YYYY-MM-DD) | `2024-01-16` |
| `category` | string | `Food` |

`settlement_period` is a **calculated field** (not present in the CSV). It is computed during ingestion in `MM-YYYY` format, representing when the transaction is actually settled. For credit card installments, it offsets the purchase date by the installment number.

*Note: Files must be pipe-delimited (`|`).*

Additional columns are ignored. Malformed rows are skipped; malformed files are skipped entirely (the endpoint will **not** return 500 due to a single bad file).

---

## Running Tests

```bash
# Run all tests with coverage report
pytest

# Run only unit tests
pytest tests/unit/

# Run only integration tests
pytest tests/integration/

# Run with verbose output
pytest -v
```

Tests require at least **80% code coverage** (enforced by `pytest-cov`).

---

## Project Structure

```
big-julius-data-ingestion/
├── app/
│   ├── main.py                  # FastAPI app entrypoint
│   ├── routers/
│   │   └── ingestion.py         # /process-files endpoint
│   ├── services/
│   │   ├── s3_service.py        # S3 interaction logic
│   │   ├── firestore_service.py # Firestore read/write logic
│   │   └── ingestion_service.py # Core business logic
│   ├── models/
│   │   └── transaction.py       # Pydantic models
│   ├── core/
│   │   ├── config.py            # Settings via pydantic-settings
│   │   └── logging.py           # Logging configuration
│   └── utils/
│       └── csv_parser.py        # CSV parsing helpers
├── tests/
│   ├── unit/
│   │   ├── test_csv_parser.py
│   │   ├── test_ingestion_service.py
│   │   └── test_firestore_service.py
│   └── integration/
│       └── test_process_files.py
├── .env.example
├── Dockerfile
├── docker-compose.yml
├── pyproject.toml
└── README.md
```

---

## Known Limitations

- Firestore deduplication queries perform one read per record, which may be slow for large batches (thousands of records). Future improvement: batch dedup using Firestore `in` queries.
- The service does not support pagination for very large S3 prefixes (> ~10,000 files per prefix). The paginator handles this correctly but memory usage may grow proportionally to CSV size.
- Firestore does not support transactional writes across batch operations, so a partial failure in `bulk_insert_temp` may leave orphan documents in the temp collection. In practice, any orphans are removed in step 4.
- The service does not validate the `date` field format beyond storing it as a string.
