from waldur_slurm import logger
from waldur_slurm.slurm_waldur_utils import slurm_waldur_sync
from waldur_slurm.waldur_slurm_utils import waldur_slurm_sync

from . import WALDUR_SYNC_DIRECTION, WaldurSyncDirection, waldur_slurm_agent_version


def main():
    logger.info("Waldur SLURM Agent version: %s", waldur_slurm_agent_version)

    logger.info("Running agent in %s mode", WALDUR_SYNC_DIRECTION)
    if WALDUR_SYNC_DIRECTION == WaldurSyncDirection.PULL.value:
        waldur_slurm_sync()
    else:
        slurm_waldur_sync()


if __name__ == "__main__":
    main()
