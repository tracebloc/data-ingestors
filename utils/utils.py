import glob
import os


DS_BACKEND = "https://dev-api.tracebloc.io/"
DEV_BACKEND = "https://dev-api.tracebloc.io/"
STG_BACKEND = "https://stg-api.tracebloc.io/"
LOCAL_BACKEND = "http://127.0.0.1:8000/"
# prduction url
PROD_BACKEND = "https://api.tracebloc.io/"



def get_files_with_extensions(directory_path, extensions):
    """
    Scan a directory and get all files with specified extensions.

    Args:
    - directory_path (str): Path to the directory to scan.
    - extensions (list): List of file extensions to look for.

    Returns:
    - files (list): List of file paths with specified extensions.
    """
    files = []
    for extension in extensions:
        files.extend(glob.glob(os.path.join(directory_path, extension)))
    return files
