from pathlib import Path

config_path = Path("./config.yaml")
yaml_str = """\
# Server information
host: localhost
port: 3306


# OrpheusPlus Path
orpheusplus_root_dir: # Add your OrpheusPlus root directory here.
"""

if not config_path.is_file():
    with open(config_path, "w") as f:
        f.write(yaml_str)