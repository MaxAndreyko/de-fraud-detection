import os
import pandas as pd
import re
from typing import List, Dict

def get_filepaths_by_pattern(source_dir: str, pattern: str) -> List[str]:
    """Gets filepaths of files with specified filename pattern using regex.

    Parameters
    ----------
    source_dir : str
        Folder where to search for files.
    pattern : str
        Regex pattern to match filenames against, e.g., r'transactions_(\d{2})(\d{2})(\d{4})\.txt'.

    Returns
    -------
    List[str]
        A list of filepaths that match the specified pattern.
    """
    matched_filepaths = []
    regex = re.compile(pattern)

    for dirpath, _, filenames in os.walk(source_dir):
        for filename in filenames:
            if regex.match(filename):
                matched_filepaths.append(os.path.join(dirpath, filename))

    return matched_filepaths


def get_incoming_data(source_dir: str, file_patterns: Dict[str, str], csv_sep: str = ";") -> Dict[str, pd.DataFrame]:
    """Collects and consolidates incoming data from files matching specified patterns.

    This function scans a specified directory for files that match the provided patterns,
    reads the data into pandas DataFrames, and consolidates them into a single DataFrame 
    for each table name. The resulting DataFrames are stored in a dictionary where the 
    keys are table names and the values are the concatenated DataFrames.

    Parameters
    ----------
    source_dir : str
        The directory path where files are located.
    file_patterns : Dict[str, str]
        A dictionary where keys are table names and values are filename patterns to match 
        against, e.g., {'sales': '*.xlsx', 'inventory': '*.csv'}.
    csv_sep : str, optional
        The separator used for reading CSV and TXT files. The default is ';'.

    Returns
    -------
    Dict[str, pd.DataFrame]
        A dictionary where each key is a table name and each value is a pandas DataFrame 
        containing the consolidated data read from the matching files.
    """


    dataframes = {}

    for table_name, pattern in file_patterns.items():
        filepaths = get_filepaths_by_pattern(source_dir, pattern)
        
        for filepath in filepaths:
            curr_data = None
            if filepath.endswith(".xlsx"):
                curr_data = pd.read_excel(filepath, header=0)
            elif filepath.endswith(".txt") or filepath.endswith(".csv"):
                curr_data = pd.read_csv(filepath, header=0, sep=csv_sep)
            if curr_data is not None:
                read_data = dataframes.get(table_name)
                if isinstance(read_data, pd.DataFrame):
                    dataframes[table_name] = pd.concat([read_data, curr_data], axis=0)
                else:
                    dataframes[table_name] = curr_data

    return dataframes
    
def prep_incoming_data(data: Dict[str, pd.DataFrame], prep_config: Dict[str, Dict]) -> List[Dict[str, pd.DataFrame]]:
    """Prepares incoming data by applying specified cleaning configurations.

    This function iterates over a dictionary of DataFrames and applies preparation 
    configurations based on the provided settings. Specifically, it cleans numeric 
    columns for each DataFrame according to the configuration defined for that table.

    Parameters
    ----------
    data : Dict[str, pd.DataFrame]
        A dictionary where keys are table names and values are pandas DataFrames 
        containing the incoming data to be prepared.
    prep_config : Dict[str, Dict]
        A dictionary containing preparation configurations for each table. Each key 
        corresponds to a table name and maps to another dictionary with preparation 
        options (e.g., which columns to clean).

    Returns
    -------
    List[Dict[str, pd.DataFrame]]
        A list of dictionaries where each dictionary contains the cleaned DataFrames 
        mapped by their respective table names.
    """

    for table_name, df in data.items():
        table_prep_config = prep_config.get(table_name)
        if table_prep_config is not None:
            numeric_cols = table_prep_config.get("numeric_cols")
            if numeric_cols is not None:
                df = clean_numeric_columns(df, numeric_cols)
    return data


def clean_numeric_columns(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """Cleans specified numeric columns in a DataFrame by standardizing their formats.

    This function takes a pandas DataFrame and a list of column names, and performs 
    cleaning operations on those columns to ensure they contain valid numeric values. 
    Specifically, it replaces commas with periods and removes all non-numeric characters 
    except for the decimal point.

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame containing the data to be cleaned.
    cols : List[str]
        A list of column names in the DataFrame that should be cleaned.

    Returns
    -------
    pd.DataFrame
        The cleaned DataFrame with specified numeric columns standardized.

    Notes
    -----
    - The function modifies the DataFrame in place and returns the same DataFrame 
      with cleaned columns.
    """

    for col in cols:
        if col in df.columns:
            df.loc[:, col] = df[col].str.replace(",", ".") # Replace comma with point
            df.loc[:, col] = df[col].str.replace("[^\.\d]", "", regex=True) # Remove all non numeric character except point
    return df


