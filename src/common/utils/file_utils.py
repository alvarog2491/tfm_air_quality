import json
import shutil
from pathlib import Path
from typing import Any, Dict, List, Union

import joblib
import yaml


def load_yaml_config(
    yaml_path: Union[str, Path],
) -> Dict[str, Union[str, List[str], Dict[str, Any]]]:
    """
    Load a YAML configuration file from the specified path.

    Args:
        yaml_path (str | Path): Path to the YAML configuration file.

    Returns:
        Dict[str, Union[str, List[str], Dict[str, Any]]]: Parsed YAML content as a dictionary.

    Raises:
        FileNotFoundError: If the file does not exist.
        yaml.YAMLError: If YAML parsing fails.
    """
    path = Path(yaml_path)
    if not path.is_file():
        raise FileNotFoundError(f"Expected file not found: {yaml_path}")

    with path.open("r", encoding="utf-8") as fp:
        return yaml.safe_load(fp)


def load_json_file(json_path: Union[str, Path]) -> Dict[str, Any]:
    """
    Load and parse a JSON file from the specified path.

    Args:
        json_path (str | Path): Path to the JSON file.

    Returns:
        Dict[str, Any]: Parsed JSON content as a dictionary.

    Raises:
        FileNotFoundError: If the file does not exist.
        json.JSONDecodeError: If the file content is not valid JSON.
    """
    path = Path(json_path)
    if not path.is_file():
        raise FileNotFoundError(f"Expected file not found: {json_path}")

    with path.open("r", encoding="utf-8") as f:
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


def load_pickle_file(file_path: str) -> Any:
    """
    Load a pickle file from the specified path.

    Args:
        file_path (Path): Path to the pickle file.

    Returns:
        Any: The loaded data.

    Raises:
        FileNotFoundError: If the file does not exist.
    """
    path = Path(file_path)
    if not path.is_file():
        raise FileNotFoundError(f"Expected file not found: {file_path}")

    return joblib.load(file_path)


def save_pickle_file(data: Any, file_path: str) -> None:
    """
    Save data to a pickle file at the specified path.

    Args:
        data (Any): Data to save.
        file_path (Path): Path where the data should be saved.
    """
    create_directory(file_path)
    joblib.dump(data, file_path)
