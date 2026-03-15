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
    
    count = 0
    for rule in rules:
        doc_id = rule["_doc_id"]
        subcat = str(rule.get("manual_subcategory") or "").strip()
        
        if subcat.lower() == "others":
            firestore.update_rule(doc_id, {"manual_subcategory": "Outros"})
            print(f"Updated: {rule.get('description')}")
            count += 1
            
    print(f"Done. Updated {count} rules.")

if __name__ == "__main__":
    main()
