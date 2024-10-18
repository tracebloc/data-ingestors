import requests
import json
from datetime import datetime
from database import db_connection  # Assume this is the database connection handler
from tqdm import tqdm
from utils import DEV_BACKEND, backend_urls
import logging

# Set up logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Function to convert string to datetime
def convert_to_datetime(date_string, date_format="%m/%d/%Y %H:%M"):
    try:
        return datetime.strptime(date_string, date_format)
    except ValueError:
        logger.error(f"Date conversion error for string: {date_string}")
        return None


class GenericDBAPI:
    def __init__(
        self,
        config,
        trigger_send_to_server=50,
        user_name="",
        password="",
        env="dev",
    ):
        """Initialize the API client with configuration settings."""
        self.url = backend_urls.get(env, DEV_BACKEND)
        self.user_name = user_name
        self.password = password
        self.config = config
        self.auth_token = self.get_auth_token()
        self.data = []
        self.trigger_send_to_server = trigger_send_to_server
        logger.info("Token authorized successfully.")

    def get_auth_token(self):
        """Authenticate with the backend and get an auth token."""
        try:
            response = requests.post(
                f"{self.url}api-token-auth/",
                data={"username": self.user_name, "password": self.password},
            )
            response.raise_for_status()
            self.auth_token = response.json().get("token")
        except requests.RequestException as e:
            logger.error(f"Failed to get auth token: {e}")
            raise
        return self.auth_token

    def send_meta_data(self):
        """Send collected metadata to the backend."""
        if self.auth_token is None:
            self.auth_token = self.get_auth_token()

        url_with_dataset_type = f"{self.url}meta_data_endpoint/"
        headers = {
            "Authorization": f"TOKEN {self.auth_token}",
            "Content-Type": "application/json",
        }

        try:
            response = requests.post(
                url_with_dataset_type, headers=headers, data=json.dumps(self.data)
            )
            if response.status_code == 201:
                self.data.clear()  # Clear data after successful send
                logger.info("Data sent successfully.")
            else:
                logger.error(
                    f"Error in API response: {response.status_code} {response.text}"
                )
        except requests.RequestException as e:
            logger.error(f"Exception during API call: {e}")

    def flush_data(self):
        """Flush any remaining unsent data."""
        if self.data:
            logger.info("Flushing remaining data...")
            self.send_meta_data()

    def process_csv(self, data, output_path):
        """Process CSV data and store it in a directory."""
        output_path.mkdir(
            parents=True, exist_ok=True
        )  # Create folder if it doesn't exist
        for item in data:
            try:
                self.insert_to_db(row=item)
            except Exception as err:
                logger.error(f"Error processing row: {err}")
        self.flush_data()  # Flush any remaining data

    def insert_to_db(self, row_data):
        """Insert data into the database and buffer for sending to backend."""
        record_status = 1  # Generic status
        company = self.config.get("company", "GenericCompany")
        created_date = datetime.now()
        updated_date = datetime.now()
        serial_number = row_data["serialNo"]

        try:
            with db_connection.cursor(buffered=True) as cursor:
                cursor.execute(
                    f"SELECT * FROM {self.config.get('table_name', 'inspections_table')} WHERE serialNo=%s",
                    (serial_number,),
                )
                result = cursor.fetchone()

                if result is None:
                    sql = f"""INSERT INTO {self.config.get('table_name', 'inspections_table')} (
                        serialNo, Field1, Field2, Field3, Field4, Field5, Field6, 
                        Field7, Field8, Field9, Field10, timestamp_x, timestamp_y, label, company, 
                        status, created_date, updated_date
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

                    # Convert timestamps
                    row_data["timestamp_x"] = convert_to_datetime(
                        row_data["timestamp_x"]
                    )
                    row_data["timestamp_y"] = convert_to_datetime(
                        row_data["timestamp_y"]
                    )
                    label = row_data["label"]

                    # Prepare data for insertion
                    val = tuple(row_data.values())
                    extra_fields = (
                        label,
                        company,
                        record_status,
                        created_date,
                        updated_date,
                    )
                    data = (*val, *extra_fields)

                    cursor.execute(sql, data)
                    db_connection.commit()
                    inserted_id = cursor.lastrowid

                    # Append data for sending to backend
                    self.data.append(
                        {
                            "data_id": inserted_id,
                            "company": company,
                            "label": label,
                            "data_intent": row_data.get(
                                "data_intent", "generic_intent"
                            ),
                            "is_sample": False,
                            "is_active": record_status,
                        }
                    )

                    # Send records to backend if buffer reaches threshold
                    if len(self.data) >= self.trigger_send_to_server:
                        self.send_meta_data()

        except Exception as e:
            logger.error(f"Error inserting data into the database: {e}")


if __name__ == "__main__":
    # Example of how to initialize and use the class
    config = {
        "company": "GenericCompany",
        "table_name": "inspections_table",
    }
    api_obj = GenericDBAPI(
        config=config, user_name="your_username", password="your_password"
    )
    logger.info(f"Auth token: {api_obj.get_auth_token()}")
