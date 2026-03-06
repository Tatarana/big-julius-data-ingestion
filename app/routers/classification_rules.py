"""Router for the /classification-rules endpoints."""

import logging
from typing import List

from fastapi import APIRouter, Depends, HTTPException, status

from app.models.transaction import (
    ClassificationRule,
    ClassificationRuleResponse,
)
from app.services.firestore_service import (
    FirestoreService,
    FirestoreServiceError,
    build_firestore_client,
)

logger = logging.getLogger(__name__)

router = APIRouter()


def get_firestore_service() -> FirestoreService:
    """Dependency provider for FirestoreService using production clients.

    Returns:
        A fully configured FirestoreService instance.
    """
    from app.core.config import settings  # noqa: PLC0415

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


@router.post(
    "/classification-rules",
    response_model=ClassificationRuleResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Add a new classification rule",
    description=(
        "Creates a new classification rule mapping a description pattern "
        "to a manual category. The rule will be applied automatically "
        "during future ingestion runs."
    ),
)
def add_classification_rule(
    rule: ClassificationRule,
    firestore: FirestoreService = Depends(get_firestore_service),
) -> ClassificationRuleResponse:
    """Create a new classification rule.

    Args:
        rule: ClassificationRule with description and manual_category.
        firestore: Injected FirestoreService dependency.

    Returns:
        ClassificationRuleResponse with the created rule's ID.

    Raises:
        HTTPException: 500 if the Firestore write fails.
    """
    try:
        doc_id = firestore.add_rule(rule.model_dump())
        return ClassificationRuleResponse(
            id=doc_id,
            description=rule.description,
            manual_category=rule.manual_category,
        )
    except FirestoreServiceError as exc:
        logger.error("Failed to add classification rule: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to add rule: {exc}",
        ) from exc


@router.get(
    "/classification-rules",
    response_model=List[ClassificationRuleResponse],
    status_code=status.HTTP_200_OK,
    summary="List all classification rules",
    description="Returns all classification rules from the Firestore collection.",
)
def list_classification_rules(
    firestore: FirestoreService = Depends(get_firestore_service),
) -> List[ClassificationRuleResponse]:
    """Retrieve all classification rules.

    Args:
        firestore: Injected FirestoreService dependency.

    Returns:
        A list of ClassificationRuleResponse objects.
    """
    rules = firestore.get_all_rules()
    return [
        ClassificationRuleResponse(
            id=r["_doc_id"],
            description=r["description"],
            manual_category=r["manual_category"],
        )
        for r in rules
    ]
