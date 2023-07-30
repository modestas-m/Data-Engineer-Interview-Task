FROM apache/airflow:2.5.0
COPY requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt
COPY homework-data2020-8863b8b21ea5.json /bigquery_key.json
ENV GOOGLE_APPLICATION_CREDENTIALS bigquery_key.json



