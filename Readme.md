# Data Ingestors
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## 📄 Description
A robust data ingestion framework for machine learning pipelines. This repository provides tools and utilities for managing, processing, and validating training/test datasets. It supports various data sources, formats, and processing pipelines, making it easier to create and maintain ML datasets.
Also, it sends the following data to tracebloc:
- a dataset id (created for the entire dataset)
- list of unique ids per data row (samples)
- list of all raw labels
- registered organisation name 
- intent e.g. training or testing
- example data points that will be shared to the user 
                  
## 🛠️ Tech Stack
- Python 3.x
- Docker (for containerization)
- Data processing libraries (Pandas, NumPy)

## 🚀 Installation & Usage Instructions
1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r src/requirements.txt
   ```
3. Configure your environment
4. Follow the documentation guide to [Create Your Training/Test Dataset](https://traceblocdocsdev.azureedge.net/environment-setup/create-your-dataset)

## 📦 Features
- Multi-source data ingestion
- Data validation and preprocessing
- Database integration
- API endpoints for data management
- Containerized deployment
- Kubernetes support


## 📜 License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 📞 Support
For additional support or questions, please refer to our documentation or contact the Tracebloc support team at `support@tracebloc.io`.
