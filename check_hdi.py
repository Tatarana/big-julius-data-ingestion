import sys
import os

from app.core.config import settings
from app.services.firestore_service import build_firestore_client, FirestoreService

def get_firestore_service() -> FirestoreService:
    firestore_client = build_firestore_client(
        project_id=settings.firestore_project_id,
        database=settings.firestore_database_id,
        credentials_path=settings.google_application_credentials,
    )
    return FirestoreService(
        client=firestore_client,
        main_collection=settings.collection_main,
        temp_collection=settings.collection_temp,
        rules_collection=settings.collection_rules,
    )

def main():
    firestore = get_firestore_service()
    rules_ref = firestore._client.collection(settings.collection_rules)
    docs = rules_ref.where("description", "==", "HDI SEGUROS SA").stream()
    for doc in docs:
        print(f"ID: {doc.id} | DATA: {doc.to_dict()}")

if __name__ == "__main__":
    main()
