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
    
    print(f"Total rules to check: {len(rules)}")
    updated_count = 0

    for rule in rules:
        doc_id = rule["_doc_id"]
        cat = rule.get("manual_category")
        subcat = rule.get("manual_subcategory")
        
        new_cat = cat
        new_subcat = subcat
        changed = False

        # 1. Mercado | Supermercado -> Compras | Supermercado
        if cat == "Mercado" and subcat == "Supermercado":
            new_cat = "Compras"
            new_subcat = "Supermercado"
            changed = True

        # 2. Viagem | * -> Entretenimento | Viagem
        elif cat == "Viagem":
            new_cat = "Entretenimento"
            new_subcat = "Viagem"
            changed = True

        # 3. Transporte | Pedágio -> Transporte | Pedágio/Tag
        elif cat == "Transporte" and subcat == "Pedágio":
            new_subcat = "Pedágio/Tag"
            changed = True

        # 4. Saúde | Consulta -> Saúde | Consulta Médica
        elif cat == "Saúde" and subcat == "Consulta":
            new_subcat = "Consulta Médica"
            changed = True

        if changed:
            print(f"Updating Rule ID {doc_id} ('{rule.get('description')}'): [{cat} | {subcat}] -> [{new_cat} | {new_subcat}]")
            firestore.update_rule(doc_id, {
                "manual_category": new_cat,
                "manual_subcategory": new_subcat
            })
            updated_count += 1

    print(f"\nBulk update complete. Total rules updated: {updated_count}")

if __name__ == "__main__":
    main()
