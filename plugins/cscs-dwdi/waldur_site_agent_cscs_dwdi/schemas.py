"""Backend settings schemas for the cscs-dwdi-{compute,inference,storage} backends."""

from __future__ import annotations

from typing import Optional

from pydantic import Field

from waldur_site_agent.common.plugin_schemas import CommonBackendSettingsSchema


class CSCSDWDIBackendSettingsSchema(CommonBackendSettingsSchema):
    """Settings for the compute and inference reporting backends."""

    cscs_dwdi_api_url: str = Field(..., description="CSCS-DWDI API base URL")
    cscs_dwdi_client_id: str = Field(..., description="OIDC client id")
    cscs_dwdi_client_secret: str = Field(..., description="OIDC client secret")
    cscs_dwdi_oidc_token_url: str = Field(..., description="OIDC token endpoint")
    cscs_dwdi_oidc_scope: Optional[str] = Field(default=None, description="OIDC scope")
    cscs_dwdi_cluster: Optional[str] = Field(
        default=None, description="Cluster to filter usage by when the resource names none"
    )
    socks_proxy: Optional[str] = Field(
        default=None, description="Proxy for the API, e.g. socks5://localhost:12345"
    )
    storage_filesystem: Optional[str] = Field(default=None, description="Storage filesystem")
    storage_data_type: Optional[str] = Field(default=None, description="Storage data type")
    storage_tenant: Optional[str] = Field(default=None, description="Storage tenant")
    storage_path_mapping: Optional[dict[str, str]] = Field(
        default=None, description="Map of resource backend id to storage path"
    )


class CSCSDWDIStorageBackendSettingsSchema(CSCSDWDIBackendSettingsSchema):
    """Settings for the storage reporting backend, which needs the storage keys."""

    storage_filesystem: str = Field(..., description="Storage filesystem")
    storage_data_type: str = Field(..., description="Storage data type")
