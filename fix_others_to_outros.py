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
    
    # Query all rules to find those with 'others' subcategory
    # Firestore is case sensitive, so 'others' is exactly what we used
    rules = firestore.get_all_rules()
    
    count = 0
    print("Starting update of manual_subcategory from 'others' to 'Outros'...")
    
    for rule in rules:
        doc_id = rule["_doc_id"]
        subcat = rule.get("manual_subcategory")
        
        if subcat == "others":
            firestore.update_rule(doc_id, {"manual_subcategory": "Outros"})
            print(f"Updated rule ID {doc_id}: '{rule.get('description')}'")
            count += 1
            
    print(f"\nSuccessfully updated {count} rules.")

if __name__ == "__main__":
    main()
