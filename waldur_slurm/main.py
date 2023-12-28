import argparse

from waldur_slurm import common_utils, logger
from waldur_slurm.slurm_waldur_utils import slurm_waldur_sync
from waldur_slurm.waldur_slurm_utils import waldur_slurm_sync

from . import WALDUR_SYNC_DIRECTION, WaldurSyncDirection


def init_argparse():
    parser = argparse.ArgumentParser(
        prog="python -m waldur_slurm.main",
        usage="%(prog)s [OPTIONS]",
        description="Waldur SLURM Agent",
    )
    parser.add_argument(
        "--diag",
        dest="diagnostics",
        help="Run diagnostics only",
        action="store_true",
    )
    parser.add_argument(
        "--load-components",
        dest="load_components",
        help="Load TRES as offering components to Waldur backend",
        action="store_true",
    )
    return parser


if __name__ == "__main__":
    parser = init_argparse()
    args = parser.parse_args()
    if args.diagnostics:
        diagnostics_status = common_utils.diagnostics()
        if not diagnostics_status:
            logger.error("Diagnostics failed")
            exit(1)
        exit(0)

    if args.load_components:
        common_utils.create_offering_components()
        exit(0)

    logger.info("Running agent in %s mode", WALDUR_SYNC_DIRECTION)
    if WALDUR_SYNC_DIRECTION == WaldurSyncDirection.PULL.value:
        waldur_slurm_sync()
    else:
        slurm_waldur_sync()
