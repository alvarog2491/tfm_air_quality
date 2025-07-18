import shutil
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union, Any
import joblib
import yaml
import json

def load_yaml_config(yaml_path: str) -> Dict[str, Union[str, List[str], Dict[str, Any]]]:
    """
    Load a YAML configuration file from the specified path.

    Parameters:
    path (str): Path to the YAML configuration file.

    Returns:
    Dict[str, Union[str, List[str], Dict[str, Any]]]: Parsed configuration as a dictionary.
    """
    if not yaml_path.is_file():
        raise FileNotFoundError(f"Expected file not found: {yaml_path}")

    with open(path, "r") as fp:
        return yaml.safe_load(fp)


def load_json_file(json_path: str) -> Any:
    """
    Load a JSON file from the specified path.

    Parameters:
    path (str): Path to the JSON file.

    Returns:
    Dict[str, Union[str, List[str], Dict[str, Any]]]: Parsed configuration as a dictionary.
    """
    if not json_path.is_file():
        raise FileNotFoundError(f"Expected file not found: {json_path}")

    with open(json_path, "r", encoding="utf-8") as f:
        return json.load(f)

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

def load_pickle_file(file_path):
    """
    Load a pickle file from the specified path.
    """
    
    if not file_path.is_file():
        raise FileNotFoundError(f"Expected file not found: {file_path}")

    return joblib.load(file_path)

def save_pickle_file(data: Any, file_path: str) -> None:
    """
    Save data to a pickle file at the specified path.

    Parameters:
    data (Any): Data to save.
    file_path (str): Path where the data should be saved.
    """
    create_directory(Path(file_path).parent)
    joblib.dump(data, file_path)
