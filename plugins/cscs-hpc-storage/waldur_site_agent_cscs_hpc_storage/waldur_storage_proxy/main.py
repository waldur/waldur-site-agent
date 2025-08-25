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


@app.exception_handler(Exception)
async def general_exception_handler(_request: Request, exc: Exception) -> JSONResponse:
    """Handle authentication and other general errors."""
    error_message = str(exc)
    logger.error("Unhandled exception in API: %s", exc, exc_info=True)

    # Check if it's an authentication-related error
    if "AuthClaimMissing" in error_message or "authentication" in error_message.lower():
        return JSONResponse(
            status_code=401,
            content={
                "detail": (
                    "Authentication failed. Please check your Bearer token and ensure it contains "
                    "required claims."
                ),
                "error": "AuthenticationError",
                "help": (
                    "The JWT token may be missing required claims like 'preferred_username', "
                    "'sub', or 'email'."
                ),
            },
        )

    # For other errors, return generic server error
    return JSONResponse(
        status_code=500,
        content={"detail": f"An error occurred: {error_message}", "error": "InternalServerError"},
    )


class User(BaseModel):
    """Model for OIDC user."""

    preferred_username: str


async def user_mapper(userinfo: dict) -> User:
    """Maps user info to a custom user structure."""
    logger.info("Received userinfo in user_mapper: %s", userinfo)
    logger.info("Available userinfo keys: %s", list(userinfo.keys()) if userinfo else "None")

    # Extract preferred_username (should be service-account-hpc-mp-storage-service-account-dci)
    preferred_username = userinfo.get("preferred_username")
    logger.info("Extracted preferred_username: %s", preferred_username)

    # Log additional useful claims for debugging
    logger.info("Client ID: %s", userinfo.get("clientId"))
    logger.info("Subject: %s", userinfo.get("sub"))
    logger.info("Roles: %s", userinfo.get("roles"))
    logger.info("Groups: %s", userinfo.get("groups"))

    if not preferred_username:
        logger.error("Missing 'preferred_username' claim in userinfo: %s", userinfo)
        # Use sub as fallback since it's always present
        fallback_username = userinfo.get("sub", "unknown_user")
        logger.warning("Using fallback username from 'sub': %s", fallback_username)
        preferred_username = fallback_username

    return User(preferred_username=preferred_username)


async def mock_user() -> User:
    """Return a mock user when auth is disabled."""
    return User(preferred_username="dev_user")


if not DISABLE_AUTH:
    logger.info("Setting up Keycloak authentication")
    logger.info("Keycloak URL: %s", CSCS_KEYCLOAK_URL)
    logger.info("Keycloak Realm: %s", CSCS_KEYCLOAK_REALM)
    logger.info("Keycloak Client ID: %s", CSCS_KEYCLOAK_CLIENT_ID)
    logger.info(
        "Keycloak Client Secret: %s", "***REDACTED***" if CSCS_KEYCLOAK_CLIENT_SECRET else "NOT SET"
    )

    if not CSCS_KEYCLOAK_CLIENT_ID or not CSCS_KEYCLOAK_CLIENT_SECRET:
        logger.error("Missing required Keycloak configuration: CLIENT_ID or CLIENT_SECRET not set")
        error_msg = "CSCS_KEYCLOAK_CLIENT_ID and CSCS_KEYCLOAK_CLIENT_SECRET must be set"
        raise ValueError(error_msg)

    keycloak_config = KeycloakConfiguration(
        url=CSCS_KEYCLOAK_URL,
        realm=CSCS_KEYCLOAK_REALM,
        client_id=CSCS_KEYCLOAK_CLIENT_ID,
        client_secret=CSCS_KEYCLOAK_CLIENT_SECRET,
        # Allow missing claims and handle them in user_mapper
        reject_on_missing_claim=False,
        # Specify required claims based on your token structure
        claims=["sub", "preferred_username", "clientId", "roles", "groups"],
        # Decode options for flexibility
        decode_options={
            "verify_signature": True,
            "verify_aud": False,  # Disable audience verification if causing issues
            "verify_exp": True,
        },
    )

    try:
        setup_keycloak_middleware(
            app,
            keycloak_configuration=keycloak_config,
            user_mapper=user_mapper,
        )
        logger.info("Keycloak middleware setup completed successfully")
    except Exception as e:
        logger.error("Failed to setup Keycloak middleware: %s", e)
        raise

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
