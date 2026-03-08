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


@router.put(
    "/classification-rules/{rule_id}",
    response_model=ClassificationRuleResponse,
    status_code=status.HTTP_200_OK,
    summary="Update a classification rule",
    description="Updates an existing classification rule by its Firestore document ID.",
)
def update_classification_rule(
    rule_id: str,
    rule: ClassificationRule,
    firestore: FirestoreService = Depends(get_firestore_service),
) -> ClassificationRuleResponse:
    """Update an existing classification rule.

    Args:
        rule_id: Firestore document ID of the rule to update.
        rule: ClassificationRule with updated description and manual_category.
        firestore: Injected FirestoreService dependency.

    Returns:
        ClassificationRuleResponse with the updated rule.

    Raises:
        HTTPException: 500 if the Firestore update fails.
    """
    try:
        firestore.update_rule(rule_id, rule.model_dump())
        return ClassificationRuleResponse(
            id=rule_id,
            description=rule.description,
            manual_category=rule.manual_category,
        )
    except FirestoreServiceError as exc:
        logger.error("Failed to update classification rule '%s': %s", rule_id, exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update rule: {exc}",
        ) from exc


@router.delete(
    "/classification-rules/{rule_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a classification rule",
    description="Deletes a classification rule by its Firestore document ID.",
)
def delete_classification_rule(
    rule_id: str,
    firestore: FirestoreService = Depends(get_firestore_service),
) -> None:
    """Delete a classification rule.

    Args:
        rule_id: Firestore document ID of the rule to delete.
        firestore: Injected FirestoreService dependency.

    Raises:
        HTTPException: 500 if the Firestore deletion fails.
    """
    try:
        firestore.delete_rule(rule_id)
    except FirestoreServiceError as exc:
        logger.error("Failed to delete classification rule '%s': %s", rule_id, exc, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete rule: {exc}",
        ) from exc
