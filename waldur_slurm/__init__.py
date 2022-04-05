import logging
import os

from waldur_client import WaldurClient

from waldur_slurm.slurm_client.backend import SlurmBackend

logger = logging.getLogger(__name__)


WALDUR_API_URL = os.environ["WALDUR_API_URL"]
WALDUR_API_TOKEN = os.environ["WALDUR_API_TOKEN"]

waldur_rest_client = WaldurClient(WALDUR_API_URL, WALDUR_API_TOKEN)

slurm_backend = SlurmBackend()
