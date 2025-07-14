import shutil
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union

import yaml


def load_yaml_config(path: str) -> Dict[str, Union[str, List[str], Dict[str, Any]]]:
    """
    Load a YAML configuration file from the specified path.

    Parameters:
    path (str): Path to the YAML configuration file.

    Returns:
    Dict[str, Union[str, List[str], Dict[str, Any]]]: Parsed configuration as a dictionary.
    """
    with open(path, "r") as fp:
        return yaml.safe_load(fp)

def reset_directory(path: str) -> None:
    """
    Delete the directory at the given path if it exists, then recreate it.

    Parameters:
    path (str): Path to the directory to reset.
    """
    try:
        shutil.rmtree(path)
    except Exception:
        pass
    finally:
        Path(path).mkdir(parents=True, exist_ok=True)

def create_directory(path: str) -> None:
    """
    Create a directory at the specified path if it does not already exist.

    Parameters:
    path (str): Path to the directory to create.
    """
    Path(path).mkdir(parents=True, exist_ok=True)
