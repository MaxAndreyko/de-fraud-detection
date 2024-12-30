import pandas as pd
from typing import List, Dict
from datetime import datetime

from py_scripts.os.utils import get_date_from_string, get_filepaths_by_pattern


def get_incoming_data(source_dir: str, file_patterns: Dict[str, str], csv_sep: str = ";") -> Dict[datetime, Dict[str, pd.DataFrame]]:
    """Collects and consolidates incoming data from files that match specified patterns.

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
    Dict[datetime, Dict[str, pd.DataFrame]]
        A dictionary where each key is a date (extracted from the filenames) and each value 
        is another dictionary. The inner dictionary maps table names to their corresponding 
        concatenated pandas DataFrames. This structure facilitates easy access to data by 
        date and table name.
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
                date = get_date_from_string(filepath)
                curr_data["path"] = [filepath] * len(curr_data) # Add column with path for further processing
                read_data = dataframes.get(date)
                if read_data is not None:
                    dataframes[date].update({table_name: curr_data})
                else:
                    dataframes[date] = {table_name: curr_data}
    dataframes = dict(sorted(dataframes.items(), key=lambda x: x[0]))
    
    return dataframes
    
def prep_incoming_data(data: Dict[datetime, Dict[str, pd.DataFrame]], prep_config: Dict[str, Dict]) -> List[Dict[str, pd.DataFrame]]:
    """Prepares incoming data by applying specified cleaning configurations.

    This function iterates over a dictionary of DataFrames and applies preparation 
    configurations based on the provided settings. Specifically, it cleans numeric 
    columns for each DataFrame according to the configuration defined for that table.

    Parameters
    ----------
    data : Dict[datetime, Dict[str, pd.DataFrame]]
        A nested dictionary where the outer keys are dates (of type datetime), 
        and the inner keys are table names. The values are pandas DataFrames 
        containing the incoming data that needs to be prepared.

    prep_config : Dict[str, Dict]
        A dictionary containing preparation configurations for each table. Each key 
        corresponds to a table name and maps to another dictionary with preparation 
        options (e.g., which columns to clean).

    Returns
    -------
    List[Dict[str, pd.DataFrame]]
        A list of dictionaries where each dictionary corresponds to a date from the input 
        data and contains the processed DataFrames for each table. Each DataFrame has been 
        modified according to the specified preparation configurations.
    """

    for _, tables in data.items():
        for table_name, df in tables.items():
            table_prep_config = prep_config.get(table_name)
            if table_prep_config is not None:
                numeric_cols = table_prep_config.get("numeric_cols")
                add_cols = table_prep_config.get("add_cols")
                rm_cols = table_prep_config.get("rm_cols")
                if numeric_cols is not None:
                    df = clean_numeric_columns(df, numeric_cols)
                if add_cols is not None:
                    df = add_columns(df, add_cols)
                if rm_cols is not None:
                    df = remove_columns(df, rm_cols)
            
    return data

def add_columns(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """
    Add specified columns to a DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame to which columns will be added.
    
    cols : List[str]
        A list of column names to be added to the DataFrame. 

    Returns
    -------
    pd.DataFrame
        A new DataFrame with the specified columns added.

    Raises
    ------
    KeyError
        If 'date' is specified in `cols` but the 'path' column is not found in the DataFrame.
    """
    
    if "date" in cols:
        if "path" in df.columns:
            df["date"] = df["path"].apply(get_date_from_string)
        else:
            raise KeyError("Column 'path' not found. Date could not be extracted.")
    return df


def remove_columns(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """
    Remove specified columns from a DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame from which columns will be removed.
    cols : List[str]
        A list of column names to be removed from the DataFrame.

    Returns
    -------
    pd.DataFrame
        A new DataFrame with the specified columns removed.
    """
    cols_to_remove = [col for col in cols if col in df.columns]
    return df.drop(columns=cols_to_remove, inplace=True)


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
