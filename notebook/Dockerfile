FROM jupyter/pyspark-notebook

USER root
RUN apt-get update && apt-get install bash

WORKDIR /home/jovyan

ENV JPMML_VERSION=1.5.4
RUN wget https://github.com/jpmml/jpmml-sparkml/releases/download/${JPMML_VERSION}/jpmml-sparkml-executable-${JPMML_VERSION}.jar \
    && mv jpmml-sparkml-executable-${JPMML_VERSION}.jar ${SPARK_HOME}/jars/jpmml-sparkml-executable-${JPMML_VERSION}.jar

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt
