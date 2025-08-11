"""API server used as proxy to Waldur storage resources."""

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from waldur_api_client.models.resource_state import ResourceState

from waldur_site_agent_cscs_hpc_storage.waldur_storage_proxy import (
    cscs_storage_backend,
    offering_config,
    waldur_client,
)

app = FastAPI()


@app.get("/api/storage-resources/")
async def storage_resources(state: ResourceState = None) -> JSONResponse:
    """Exposes list of all storage resources."""
    storage_data: dict = cscs_storage_backend.generate_all_resources_json(
        offering_config.uuid, waldur_client, state=state, write_file=False
    )
    return JSONResponse(content=storage_data)
