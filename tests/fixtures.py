from waldur_site_agent.common.structures import Offering

OFFERING = Offering(
    uuid="d629d5e45567425da9cdbdc1af67b32c",
    name="example-test-00",
    api_url="http://localhost:8081/api/",
    api_token="9e1132b9616ebfe943ddf632ca32bbb7e1109a32",
    backend_type="slurm",
    order_processing_backend="slurm",
    membership_sync_backend="slurm",
    reporting_backend="slurm",
    backend_settings={
        "default_account": "root",
        "customer_prefix": "hpc_",
        "project_prefix": "hpc_",
        "allocation_prefix": "hpc_",
        "enable_user_homedir_account_creation": True,
    },
    backend_components={
        "cpu": {
            "limit": 10,
            "measured_unit": "k-Hours",
            "unit_factor": 60000,
            "accounting_type": "limit",
            "label": "CPU",
        },
        "mem": {
            "limit": 10,
            "measured_unit": "gb-Hours",
            "unit_factor": 61440,
            "accounting_type": "usage",
            "label": "RAM",
        },
    },
)
