"""API server used as proxy to Waldur storage resources."""

from typing import Annotated, Optional

from fastapi import Depends, FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.logger import logger
from fastapi.responses import JSONResponse
from fastapi_keycloak_middleware import (
    KeycloakConfiguration,
    get_user,
    setup_keycloak_middleware,
)
from pydantic import BaseModel
from waldur_api_client.models.resource_state import ResourceState

from waldur_site_agent_cscs_hpc_storage.waldur_storage_proxy import (
    CSCS_KEYCLOAK_CLIENT_ID,
    CSCS_KEYCLOAK_CLIENT_SECRET,
    CSCS_KEYCLOAK_REALM,
    CSCS_KEYCLOAK_URL,
    DISABLE_AUTH,
    cscs_storage_backend,
    offering_config,
    waldur_client,
)

app = FastAPI()


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(
    _request: Request, exc: RequestValidationError
) -> JSONResponse:
    """Custom validation error handler with helpful messages for missing storage_system."""
    # Check if storage_system is missing from query parameters
    for error in exc.errors():
        if error.get("loc") == ["query", "storage_system"] and error.get("type") == "missing":
            return JSONResponse(
                status_code=422,
                content={
                    "detail": [
                        {
                            "type": "missing",
                            "loc": ["query", "storage_system"],
                            "msg": (
                                "storage_system is a mandatory filter parameter. "
                                "Please specify a storage system (e.g., capstor, vast, iopsstor)."
                            ),
                            "input": None,
                            "ctx": {
                                "examples": ["capstor", "vast", "iopsstor"],
                                "help": "Add ?storage_system=<system_name> to your request",
                            },
                        }
                    ]
                },
            )

    # For other validation errors, return the default FastAPI error format
    return JSONResponse(status_code=422, content={"detail": exc.errors()})


class User(BaseModel):
    """Model for OIDC user."""

    preferred_username: str


async def user_mapper(userinfo: dict) -> User:
    """Maps user info to a custom user structure."""
    return User(preferred_username=userinfo.get("preferred_username"))


async def mock_user() -> User:
    """Return a mock user when auth is disabled."""
    return User(preferred_username="dev_user")


if not DISABLE_AUTH:
    keycloak_config = KeycloakConfiguration(
        url=CSCS_KEYCLOAK_URL,
        realm=CSCS_KEYCLOAK_REALM,
        client_id=CSCS_KEYCLOAK_CLIENT_ID,
        client_secret=CSCS_KEYCLOAK_CLIENT_SECRET,
    )

    setup_keycloak_middleware(
        app,
        keycloak_configuration=keycloak_config,
        user_mapper=user_mapper,
    )

    user_dependency = get_user
else:
    logger.warning("Authentication is disabled! This should only be used in development.")
    user_dependency = mock_user

OIDCUserDependency = Annotated[User, Depends(user_dependency)]


@app.get("/api/storage-resources/")
async def storage_resources(
    user: OIDCUserDependency,
    storage_system: Annotated[
        str, Query(description="REQUIRED: Storage system filter (e.g., capstor, vast, iopsstor)")
    ],
    state: Optional[ResourceState] = None,
    page: Annotated[int, Query(ge=1, description="Page number (starts from 1)")] = 1,
    page_size: Annotated[int, Query(ge=1, le=500, description="Number of items per page")] = 100,
    data_type: Annotated[
        Optional[str], Query(description="Optional: Data type filter (users/scratch/store/archive)")
    ] = None,
    status: Annotated[
        Optional[str], Query(description="Optional: Status filter (pending/removing/active/error)")
    ] = None,
) -> JSONResponse:
    """Exposes list of all storage resources with pagination and filtering."""
    logger.info(
        "Processing request for user %s (page=%d, page_size=%d, storage_system=%s, "
        "data_type=%s, status=%s)",
        user.preferred_username,
        page,
        page_size,
        storage_system,
        data_type,
        status,
    )

    storage_data: dict = cscs_storage_backend.generate_all_resources_json(
        offering_config.uuid,
        waldur_client,
        state=state,
        write_file=False,
        page=page,
        page_size=page_size,
        storage_system=storage_system,
        data_type=data_type,
        status=status,
    )

    # Return appropriate HTTP status code based on response status
    if storage_data.get("status") == "error":
        return JSONResponse(content=storage_data, status_code=storage_data.get("code", 500))

    return JSONResponse(content=storage_data)
