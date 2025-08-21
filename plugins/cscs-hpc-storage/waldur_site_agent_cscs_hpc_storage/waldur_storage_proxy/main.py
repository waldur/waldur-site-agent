"""API server used as proxy to Waldur storage resources."""

from typing import Annotated

from fastapi import Depends, FastAPI
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
    cscs_storage_backend,
    offering_config,
    waldur_client,
)

app = FastAPI()


class User(BaseModel):
    """Model for OIDC user."""

    preferred_username: str


async def user_mapper(userinfo: dict) -> User:
    """Maps user info to a custom user structure."""
    return User(preferred_username=userinfo.get("preferred_username"))


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

OIDCUserDependency = Annotated[User, Depends(get_user)]


@app.get("/api/storage-resources/")
async def storage_resources(
    user: OIDCUserDependency,
    state: ResourceState = None,
) -> JSONResponse:
    """Exposes list of all storage resources."""
    logger.info("Processing request for user %s", user.preferred_username)
    storage_data: dict = cscs_storage_backend.generate_all_resources_json(
        offering_config.uuid, waldur_client, state=state, write_file=False
    )
    return JSONResponse(content=storage_data)
