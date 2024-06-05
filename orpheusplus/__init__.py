from pathlib import Path
import sys

import yaml

__all__ = ["ORPHEUSPLUS_ROOT_DIR", "ORPHEUSPLUS_CONFIG"]

DEFAULT_DIR = (Path.home() / ".orpheusplus").absolute()

def import_check():
    _check_config()
    _check_metadata()


def _check_config():
    # Check if orpheusplus_root_dir is properly configured before installation
    try:
        global ORPHEUSPLUS_CONFIG
        with open(DEFAULT_DIR / "config.yaml", encoding="utf-8") as f:
            ORPHEUSPLUS_CONFIG = yaml.load(f, Loader=yaml.FullLoader)
    except FileNotFoundError:
        config = {}
        config["host"] = "127.0.0.1"
        config["port"] = 3306
        config["orpheusplus_root_dir"] = DEFAULT_DIR
        config["orpheusplus_root_dir"].mkdir(exist_ok=True, parents=True)
        config["orpheusplus_root_dir"] = str(config["orpheusplus_root_dir"])
        with open(DEFAULT_DIR / "config.yaml", "w", encoding="utf-8")  as f:
            yaml.dump(config, f)
        print(
            f"First time setup:\n"
            f"Create `config.yaml` at {str(DEFAULT_DIR / 'config.yaml')}\n"
            f"Save orpheusplus data to {str(DEFAULT_DIR)}\n"
            f"Please run `orpheusplus config` again to change default directory and set up user."
        )
        sys.exit()

    if ORPHEUSPLUS_CONFIG["orpheusplus_root_dir"] is None:
        message = (
            f"orpheusplus_root_dir is not properly configured. "
            f"orpheusplus_root_dir: {ORPHEUSPLUS_CONFIG['orpheusplus_root_dir']}. "
        )
        raise ImportError(message)

    Path(ORPHEUSPLUS_CONFIG["orpheusplus_root_dir"]).mkdir(
        exist_ok=True, parents=True)
    global ORPHEUSPLUS_ROOT_DIR
    ORPHEUSPLUS_ROOT_DIR = Path(ORPHEUSPLUS_CONFIG["orpheusplus_root_dir"])


def _check_metadata():
    global ORPHEUSPLUS_ROOT_DIR
    global USER_PATH
    global VERSIONGRAPH_DIR
    global OPERATION_DIR
    global LOG_DIR
    global GROUP_DIR

    meta = ORPHEUSPLUS_ROOT_DIR / ".meta"
    USER_PATH = meta / "user"
    VERSIONGRAPH_DIR = meta / "versiongraph"
    OPERATION_DIR = meta / "operation"
    LOG_DIR = meta / "log"
    GROUP_DIR = meta / "group"

    meta.mkdir(exist_ok=True, parents=True)
    USER_PATH.touch(exist_ok=True)
    VERSIONGRAPH_DIR.mkdir(exist_ok=True)
    OPERATION_DIR.mkdir(exist_ok=True)
    LOG_DIR.mkdir(exist_ok=True)
    GROUP_DIR.mkdir(exist_ok=True)


import_check()