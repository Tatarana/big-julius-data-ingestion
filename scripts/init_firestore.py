"""Initialization script for Firestore collections.

This script performs a sentinel write operation to ensure the collections
configured in the .env file are created and visible in the GCP Console.
"""

import os
import sys
from datetime import datetime, timezone

# Add the project root to sys.path to import app modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

try:
    from app.core.config import settings
    from app.services.firestore_service import build_firestore_client
    from google.api_core.exceptions import GoogleAPICallError
except ImportError as e:
    print(f"Error: Missing dependencies or path issues. {e}")
    sys.exit(1)

def main():
    print("--- Firestore Collection Initialization ---")
    
    # Authenticate
    try:
        client = build_firestore_client(
            project_id=settings.firestore_project_id,
            database=settings.firestore_database_id,
            credentials_path=settings.google_application_credentials
        )
        print(f"Successfully connected to project: {settings.firestore_project_id}, database: {settings.firestore_database_id}")
    except Exception as e:
        print(f"FAILED to connect to Firestore: {e}")
        print("\nTIP: Ensure your .env file has correct GOOGLE_APPLICATION_CREDENTIALS path.")
        sys.exit(1)

    collections = [
        ("Main Transaction Collection", settings.collection_main),
        ("Temporary Ingestion Collection", settings.collection_temp),
    ]

    for label, name in collections:
        print(f"\nInitializing {label}: '{name}'...")
        col_ref = client.collection(name)
        
        # Sentinel write
        sentinel_id = f"init_sentinel_{int(datetime.now(timezone.utc).timestamp())}"
        try:
            doc_ref = col_ref.document(sentinel_id)
            doc_ref.set({
                "init_msg": "Initializing collection",
                "initialized_at": datetime.now(timezone.utc)
            })
            print(f"  [OK] Sentinel record written: {sentinel_id}")
            
            # Immediately delete sentinel to keep collection clean
            doc_ref.delete()
            print(f"  [OK] Sentinel record deleted (collection is now active).")
        except GoogleAPICallError as e:
            print(f"  [ERROR] Failed to initialize collection '{name}': {e}")
        except Exception as e:
            print(f"  [ERROR] An unexpected error occurred: {e}")

    print("\nInitialization complete. Your collections should now be visible in the Google Cloud Console.")

if __name__ == "__main__":
    main()
