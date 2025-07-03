import pandas as pd

def load_dataset(filepath: str):
    return pd.read_csv(filepath)
