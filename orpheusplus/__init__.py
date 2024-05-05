from pathlib import Path

import yaml

__all__ = ["ORPHEUSPLUS_ROOT_DIR", "ORPHEUSPLUS_CONFIG"]


def import_check():
    _check_config()
    _create_metadata_and_config()


def _check_config():
    # Check if orpheusplus_root_dir is properly configured before installation
    try:
        global ORPHEUSPLUS_CONFIG
        with open("./config.yaml", encoding="utf-8") as f:
            ORPHEUSPLUS_CONFIG = yaml.load(f, Loader=yaml.FullLoader)
    except FileNotFoundError:
        raise ImportError(
            "config.yaml not found in current directory. Abort import.")

    if ORPHEUSPLUS_CONFIG["orpheusplus_root_dir"] is None:
        message = (
            f"orpheusplus_root_dir is not properly configured. "
            f"orpheusplus_root_dir: {ORPHEUSPLUS_CONFIG['orpheusplus_root_dir']}. "
            f"Abort import."
        )
        raise ImportError(message)

    Path(ORPHEUSPLUS_CONFIG["orpheusplus_root_dir"]).mkdir(
        exist_ok=True, parents=True)
    global ORPHEUSPLUS_ROOT_DIR
    ORPHEUSPLUS_ROOT_DIR = Path(ORPHEUSPLUS_CONFIG["orpheusplus_root_dir"])


def _create_metadata_and_config():
    global ORPHEUSPLUS_ROOT_DIR

    (ORPHEUSPLUS_ROOT_DIR / ".meta").mkdir(exist_ok=True, parents=True)
    (ORPHEUSPLUS_ROOT_DIR / ".meta/user").touch(exist_ok=True)
    (ORPHEUSPLUS_ROOT_DIR / ".meta/versiongraph").touch(exist_ok=True)


import_check()