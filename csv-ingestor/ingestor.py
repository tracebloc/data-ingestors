import csv
import logging.config
import os
import sys
import time
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from utils import get_config
from serverapi import DBAPI


log_filename = "logs/ingestor.log"
os.makedirs(os.path.dirname(log_filename), exist_ok=True)
log_handler = TimedRotatingFileHandler(
    log_filename, when="midnight", interval=1, encoding="utf8", backupCount=10
)
log_handler.suffix = "%Y-%m-%d"
formatter = logging.Formatter(
    "%(asctime)-15s" "| %(threadName)-11s" "| %(levelname)-5s" "| %(message)s"
)
log_handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(log_handler)


if __name__ == "__main__":
    # Define default configuration
    default_config = {
        "src_path": "",
        "dest_path": "txt",
        "label_file": "label.json",
        "company": "CS",
        "table_name": "documents_data",
        "text_intent": "train",
        "edge_username": "xyz",
        "edge_password": "1234",
        "edge_env": "dev",
        "trigger_send_to_server": 50,
    }

    # Get configuration from environment variables (optional)
    config = get_config(default_config)

    # Validate source path existence
    if not os.path.exists(config["src_path"]):
        print(f'Input path "{config["src_path"]}" does not exist')
        sys.exit()

    # Create output path if it doesn't exist
    Path(config["dest_path"]).mkdir(parents=True, exist_ok=True)

    start_time = time.time()  # Start timing

    try:
        # create server connection
        server_obj = DBAPI(
            user_name=config.get("edge_username"),
            password=config.get("edge_password"),
            env=config.get("edge_env"),
            config=config,
            trigger_send_to_server=config.get("trigger_send_to_server", 50),
        )
        # Process CSV data using optimized function
        with open(config["label_file"], mode="r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            server_obj.process_csv(reader, final_path=Path(config["dest_path"]))

        print("All files have been successfully saved.")
        end_time = time.time()  # End timing
        elapsed_time = end_time - start_time
        minutes, seconds = divmod(elapsed_time, 60)
        print(f"Time taken: {int(minutes)} minutes and {seconds:.2f} seconds")
    except Exception as e:
        print(e)
