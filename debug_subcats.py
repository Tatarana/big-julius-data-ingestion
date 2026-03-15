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
    rules = firestore.get_all_rules()
    
    print(f"Total rules: {len(rules)}")
    for rule in rules:
        subcat = rule.get("manual_subcategory")
        print(f"DESC: {rule.get('description')} | SUBCAT: [{subcat}] ({type(subcat)})")

if __name__ == "__main__":
    main()
