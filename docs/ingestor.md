## CSV Ingestor Setup Steps

### Introduction
The CSV Ingestor is designed to read data from CSV files, process it, and store it in a database or send it to an API.

### Prerequisites
- Ensure you have Python installed on your machine.
- Install necessary libraries (e.g., `pandas`, `requests`, etc.) as required by your project.

### Step-by-Step Setup

1. **Import Required Libraries**:
   At the beginning of your `csv_ingestor.py`, import the necessary libraries:
   ```python
   import sys
   import os
   import logging
   from typing import Dict, Any
   ```

2. **Configuration**:
   Initialize the configuration and set up logging:
   ```python
   config = Config()
   setup_logging(config)
   logger = logging.getLogger(__name__)
   ```

3. **Database and API Client Initialization**:
   Create instances of the database and API client:
   ```python
   database = Database(config)
   api_client = APIClient(config)
   ```

4. **Schema Definition**:
   Define the schema for the data you will be ingesting:
   ```python
   schema = {
       "name": "VARCHAR(255)",
       "age": "INT",
       "email": "VARCHAR(255)",
       "description": "VARCHAR(255)",
       "profile_image_url": "VARCHAR(512)",
       "notes": "TEXT"
   }
   ```

5. **CSV Options**:
   Specify options for reading the CSV file:
   ```python
   csv_options = {
       "chunk_size": 1000,
       "delimiter": ",",
       "quotechar": '"',
       "escapechar": "\\",
   }
   ```

6. **Ingestor Creation**:
   Create an instance of the CSV ingestor with the defined schema and options:
   ```python
   ingestor = CSVIngestor(
       database=database,
       api_client=api_client,
       table_name=config.TABLE_NAME,
       schema=schema,
       csv_options=csv_options,
       unique_id_column="id",
       label_column="name",
       intent_column="data_intent",
       annotation_column="notes"
   )
   ```

7. **Data Ingestion**:
   Use the ingestor to read and process the data:
   ```python
   with ingestor:
       failed_records = ingestor.ingest(config.LABEL_FILE, batch_size=config.BATCH_SIZE)
       if failed_records:
           print(f"Failed to process {len(failed_records)} records")
   ```
