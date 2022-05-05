from waldur_slurm.slurm_waldur_utils import slurm_waldur_sync
from waldur_slurm.waldur_slurm_utils import waldur_slurm_sync

from . import WALDUR_SYNC_DIRECTION, WaldurSyncDirection

if __name__ == "__main__":
    if WALDUR_SYNC_DIRECTION == WaldurSyncDirection.PULL.value:
        waldur_slurm_sync()
    else:
        slurm_waldur_sync()
