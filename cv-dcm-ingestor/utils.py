import glob
import os
import fnmatch

DS_BACKEND = "https://xray-backend.azurewebsites.net/"
DEV_BACKEND = "https://xray-backend-develop.azurewebsites.net/"
STG_BACKEND = "https://xray-backend-staging.azurewebsites.net/"
LOCAL_BACKEND = "http://127.0.0.1:8000/"
# prduction url
PROD_BACKEND = "https://tracebloc.azurewebsites.net/"


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


def list_image_files(root_dir, extensions):
    """
    Scans all directories and subdirectories under `root_dir` to find image files with specified extensions.
    Assumes that image files are located in subdirectories named 'images'.
    Returns a list of file paths in the format "directory/images/file_name.ext".
    """
    image_files = []

    # Walk through all directories and subdirectories
    for dirpath, dirnames, filenames in os.walk(root_dir):
        # Check if the current directory is an 'images' directory
        # if os.path.basename(dirpath) == "images":
        # Construct the file path for each image file
        for filename in filenames:
            # Check if the file matches any of the desired extensions
            if any(
                fnmatch.fnmatch(filename.lower(), pattern) for pattern in extensions
            ):
                # Construct the path relative to the root directory
                rel_dir = os.path.relpath(dirpath, root_dir)
                # Construct the full file path
                file_path = os.path.join(rel_dir, filename)
                image_files.append(file_path)

    return image_files
