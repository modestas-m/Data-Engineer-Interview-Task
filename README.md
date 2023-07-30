# task
Instructions on setting up this repository on your machine
1. Download _Docker_ and _Docker-Compose.yaml_ made specifically for airflow
2. My airflow DAG requires external libraries that are not installed together when building a docker image, therefore a custom image needs to be built:
   a. Create a _Dockerfile_
   b. Populate the file with this code
   
     ```
      FROM apache/airflow:2.5.0
      COPY requirements.txt /requirements.txt
      RUN pip install --user --upgrade pip
      RUN pip install --no-cache-dir --user -r /requirements.txt
     ```
   c. Run a command to build a custom docker image
   
      ```
      docker build . -f Dockerfile --pull --tag my-image:0.0.1
      ```
3. Finally, we need to initiate airflow using the _docker-compose.yaml_ file

   ```
   docker compose up airflow init
   ```
   
