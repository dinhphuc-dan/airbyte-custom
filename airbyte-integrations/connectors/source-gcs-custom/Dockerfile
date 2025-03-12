FROM python:3.10.0-slim

WORKDIR /airbyte/integration_code

# upgrade pip to the latest version, install timezone and bash
RUN apt-get update \
    && pip install --upgrade pip \
    && apt-get install tzdata -y \
    && apt-get install bash -y

ENV AIRBYTE_ENTRYPOINT "python /airbyte/integration_code/main.py"

# install necessary packages
COPY setup.py ./
COPY main.py ./
COPY source_gcs_custom ./source_gcs_custom
RUN pip install .


ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]

LABEL io.airbyte.version=0.1.0
LABEL io.airbyte.name=airbyte/source-gcs-custom
