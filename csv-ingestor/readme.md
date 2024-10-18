
# Setup Guide

Follow these instructions to set up the MySQL pod, create the required table, and upload data to the shared PVC.

## Prerequisites

- Ensure you have access to the Kubernetes cluster.
- Make sure you have the necessary permissions to access the MySQL pod and the shared PVC.
- Have the required files (`schema.sql`, `data.csv`, and `deployment.yaml`) ready.

## Steps

### 1. SSH into the MySQL Pod

1. Locate the MySQL pod in your Kubernetes namespace. You can use the following command to list all running pods:

   ```bash
   kubectl get pods -n <namespace>
   ```

2. SSH into the MySQL pod:

   ```bash
   kubectl exec -it <mysql-pod-name> -n <namespace> -- /bin/bash
   ```

3. Once inside the pod, run the MySQL command to connect to your database:

   ```bash
   mysql -u <username> -p
   ```

   Enter the MySQL password when prompted.

4. Execute the SQL schema to create the required table:

   ```sql
   source /path/to/schema.sql;
   ```

   Make sure the `schema.sql` file is available within the pod or copy it there using:

   ```bash
   kubectl cp /local/path/to/schema.sql <namespace>/<mysql-pod-name>:/path/inside/pod
   ```

### 2. Create the `welds_inspections` Directory on the Shared PVC

1. Access the node where the shared PVC is mounted or SSH into a pod that has access to it.

2. Navigate to the shared PVC directory path.

3. Create the `welds_inspections` directory:

   ```bash
   mkdir -p /path/to/shared-pvc/welds_inspections
   ```

4. Upload the `data.csv` file to the `welds_inspections` directory. You can use `kubectl cp` to copy the file from your local system:

   ```bash
   kubectl cp /local/path/to/data.csv <namespace>/<pod-name>:/path/to/shared-pvc/welds_inspections
   ```

### 3. Update the `deployment.yaml` File

1. Open the `deployment.yaml` file in your preferred text editor.

2. Add the required credentials (e.g., username, password) under the environment variables or secret references section, as needed. An example:

   ```yaml
   env:
     - name: DB_USERNAME
       value: "<your-username>"
     - name: DB_PASSWORD
       value: "<your-password>"
   ```

   Make sure that any sensitive information is securely stored using Kubernetes secrets if applicable.

3. Save the changes and deploy the updated configuration:

   ```bash
   kubectl apply -f deployment.yaml
   ```
