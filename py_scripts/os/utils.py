import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


def get_date_from_string(
    string: str,
    regexp_date_pattern: str = r"(\d{2})(\d{2})(\d{4})",
    dt_date_pattern: str = "%d%m%Y",
) -> datetime:
    """
    Extracts a date from a string and returns it as a datetime object.

    Parameters
    ----------
    string : str
        The input string from which to extract the date.
    regexp_date_pattern : str
        Regular expression pattern to match the date.
    dt_date_pattern : str
        Format of the date to be extracted.

    Returns
    -------
    datetime
        The extracted date as a datetime object.

    Raises
    ------
    ValueError
        If no valid date is found in the string.
    """
    match = re.search(regexp_date_pattern, string)
    if match:
        date_str = "".join(match.groups())
        return datetime.strptime(date_str, dt_date_pattern)
    raise ValueError("No valid date found in the string.")


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


def archive_files_by_patterns(
    data_folder: str, archive_folder: str, patterns: Dict[str, Any]
) -> None:
    """
    Moves files matching specified patterns from data_folder to archive_folder.

    Parameters
    ----------
    data_folder : str
        The path to the folder containing the files to be archived.
    archive_folder : str
        The path to the folder where files will be moved.
    patterns : dict of {str: Any}
        A dictionary where keys are pattern names and values are the patterns used
        to match files in the data folder.

    Returns
    -------
    None
        This function does not return a value. It modifies the filesystem by moving files.
    """
    archive_path = Path(archive_folder)
    archive_path.mkdir(parents=True, exist_ok=True)

    for _, pattern in patterns.items():
        for file_path in get_filepaths_by_pattern(data_folder, pattern):
            src_path = Path(file_path)
            new_filename = f"{src_path.name}.backup"
            destination_path = archive_path / new_filename

            src_path.rename(destination_path)
