# The Data Ingestor

## Official Documentation
Follow the [official documentation](https://docs.tracebloc.io/create-use-case/prepare-dataset) to add your training/test dataset to your tracebloc client.

## Features
- Multi-source data ingestion
- API endpoints for data management
- Containerized deployment
- Kubernetes support

## License
This project is licensed under the MIT License [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT).

## Support
For additional support or questions, please refer to our documentation or contact the Tracebloc support team at `support@tracebloc.io`.



## Injest Sample Data for Tabular Regression


## Copy data file

`kubectl cp --no-preserve -c api <PATH_TO_DATA_INGESTORS_REPO>/data-ingestors/templates/tabular_regression/tabular_regression_sample_in_csv_format.csv <NAMESPACE>/<JOBS_MANAGER_POD_NAME>:/data/shared/`

Eg:

`kubectl cp --no-preserve -c api /Users/syedsaqlain/ProductApps/data-ingestors/templates/tabular_regression/tabular_regression_sample_in_csv_format.csv saqlain-local-cluster/saqlain-local-cluster-release-jobs-manager-594d9f7c45-g9vxz:/data/shared/`


### Injest Train Data

- Update `ingestor-job.yaml`

- `docker build --platform linux/arm64 --no-cache -t tracebloc/<CHANGE_ME TAG_NAME>-tabular-regression-sample-ingestor-train-data:latest .`

- `docker push tracebloc/<CHANGE_ME TAG_NAME>-tabular-regression-sample-ingestor-train-data:latest`

- `kubectl apply -f ingestor-job.yaml`

- `kubectl delete job ingestor-job-train-data -n <CHANGE_ME NAMESPACE>`

### Injest Test Data

- Update `ingestor-job-test.yaml`

- Update `templates/tabular_regression/tabular_regression.py` set `intent=Intent.TEST`

- `docker build --platform linux/arm64 --no-cache -t tracebloc/<CHANGE_ME TAG_NAME>-tabular-regression-sample-ingestor-test-data:latest .`

- `docker push tracebloc/<CHANGE_ME TAG_NAME>-tabular-regression-sample-ingestor-test-data:latest`

- `kubectl apply -f ingestor-job-test.yaml`

- `kubectl delete job ingestor-job-test-data -n <CHANGE_ME NAMESPACE>`