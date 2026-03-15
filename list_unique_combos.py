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
    
    combinations = set()
    for rule in rules:
        cat = rule.get("manual_category") or "Desconhecido"
        subcat = rule.get("manual_subcategory") or "Outros"
        combinations.add(f"{cat} | {subcat}")

    # Sorting for better readability
    sorted_combinations = sorted(list(combinations))

    output_file = "category_subcategory_combinations.txt"
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("Unique combinations of Category | Subcategory:\n")
        f.write("-" * 45 + "\n")
        for combo in sorted_combinations:
            f.write(combo + "\n")
            
    print(f"File created: {output_file}")
    print(f"Total unique combinations: {len(sorted_combinations)}")

if __name__ == "__main__":
    main()
