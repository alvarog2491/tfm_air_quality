import shutil
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union

import pandas as pd
import yaml


def load_config(path: str) -> Dict[str, Union[str, List[str], Dict[str, Any]]]:
    """Load a yaml file from a given path and return a dictionary."""
    with open(path, "r") as fp:
        return yaml.safe_load(fp)


def delete_and_recreate_dir(path: str) -> None:
    """Delete and recreate a directory."""
    try:
        shutil.rmtree(path)
    except:
        pass
    finally:
        Path(path).mkdir(parents=True, exist_ok=True)


def load_data(
    file_path: str, target_column: str, shuffle: bool, shuffle_random_state: int
) -> Tuple[pd.DataFrame, pd.Series]:
    """Load a dataset from a given file path and return X and y."""
    data = pd.read_csv(file_path)

    if shuffle:
        data = data.sample(frac=1, random_state=shuffle_random_state).reset_index(
            drop=True
        )

    X = data.drop(target_column, axis=1)
    y = data[target_column]
    return X, y
