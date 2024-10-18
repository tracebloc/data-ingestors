## Steps to Run Project
To run this project follow these steps:
* Clone Repository
```
git clone https://github.com/tracebloc/data-ingestors.git
```
* Check required directory inside "/data/shared/" and files are present
    * input_images : containing all images
    * label.csv
* Create required directory 
    * raw_images
    * processed_images
* Activate Python Environment
* Move to repository
```commandline
cd data-ingestors/csv-ingestor
```
* Run command to build docker image 
```commandline
docker build -t IMAGE_NAME:TAG PATH_TO_DOCKERFILE .
```
* Run command to push docker image
```commandline
docker push IMAGE_NAME:TAG PATH_TO_DOCKERFILE
```
* Run command to start ingestor deployment "deployment.yaml"
```commandline
kubectl apply -f deployment.yaml
```
* Move all images from input_images to raw_images
```commandline
mv input_images/* raw_images
```
