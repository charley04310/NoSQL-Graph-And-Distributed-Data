FROM apache/spark:3.5.5

ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_BUNDLE_VERSION=1.11.1026
ENV NEO4J_CONNECTOR_VERSION=5.3.5_for_spark_3

RUN mkdir -p /opt/spark/jars && \
    curl -L -o /opt/spark/jars/hadoop-aws.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    curl -L -o /opt/spark/jars/aws-java-sdk-bundle.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_BUNDLE_VERSION}.jar \
    curl -L -o /opt/spark/jars/neo4j-connector-apache-spark.jar https://repo1.maven.org/maven2/org/neo4j/neo4j-connector-apache-spark_2.12/${NEO4J_CONNECTOR_VERSION}/neo4j-connector-apache-spark_2.12-5.3.5_for_spark_3.jar  

COPY README.md /opt/spark/python/README.md
COPY main.py /opt/spark/python/main.py
COPY main-csv.py /opt/spark/python/main-csv.py
COPY main-neo4j.py /opt/spark/python/main-neo4j.py
