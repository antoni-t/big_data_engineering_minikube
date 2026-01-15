FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir requests==2.32.3
COPY upload_repo_data_to_hdfs.py /app/upload_repo_data_to_hdfs.py
ENTRYPOINT ["python", "/app/upload_repo_data_to_hdfs.py"]
