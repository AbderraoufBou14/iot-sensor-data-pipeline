FROM spark:3.5.1-python3

USER root

WORKDIR /opt/spark-apps

COPY spark/jobs/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY spark/jobs ./jobs/

ENTRYPOINT ["python3", "-u", "jobs/consumer.py"]