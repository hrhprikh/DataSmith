import pandas as pd

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Automatically cleans a DataFrame:
    - normalizes column names
    - removes duplicates
    - fills missing values
    - strips text spaces
    """
    df = df.copy()

    # Normalize column names
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

    # Remove duplicate rows
    df = df.drop_duplicates()

    # Fill missing values
    for col in df.columns:
        if df[col].dtype == 'O':  # object/string columns
            df[col] = df[col].fillna("Unknown")
        else:                     # numeric columns
            df[col] = df[col].fillna(df[col].mean())

    # Strip whitespace from string columns
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].str.strip()

    return df
