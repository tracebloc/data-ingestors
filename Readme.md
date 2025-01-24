
## Steps to Set Up Your Own Ingestor

1. **Clone Repository**:
```
git clone https://github.com/tracebloc/data-ingestors.git
```

2. **Create Your Ingestor**: 
   - Use the provided examples in `src/examples/csv_ingestor.py` as a reference to create your own ingestor. 
   - Ensure you define the schema, processors, and any specific options required for your data.

3. **Create a Python Virtual Environment**:
   - Navigate to your project directory.
   - Run the following command to create a virtual environment:
     ```commandline
     python -m venv venv
     ```
   - Activate the virtual environment:
     - On Windows:
       ```commandline
       venv\Scripts\activate
       ```
     - On macOS and Linux:
       ```commandline
       source venv/bin/activate
       ```

4. **Update the Deployment Configuration**:
   - Open the `deployment.yaml` file.
   - Modify the environment variables and other configurations to match your ingestor setup. Ensure to update the image name and any paths that are specific to your ingestor.

5. **Build the Docker Image**:
   - Navigate to the directory containing your Dockerfile.
   - Run the following command to build your Docker image:
     ```commandline
     docker build -t YOUR_IMAGE_NAME:YOUR_TAG PATH_TO_DOCKERFILE .
     ```

6. **Push the Docker Image**:
   - After building the image, push it to your Docker registry using:
     ```commandline
     docker push YOUR_IMAGE_NAME:YOUR_TAG
     ```

7. **Deploy Your Ingestor**:
   - Use the updated `deployment.yaml` to deploy your ingestor with Kubernetes:
     ```commandline
     kubectl apply -f deployment.yaml
     ```
