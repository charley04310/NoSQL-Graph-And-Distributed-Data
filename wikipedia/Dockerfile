FROM apache/spark:3.5.5

ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_BUNDLE_VERSION=1.11.1026


RUN mkdir -p /opt/spark/jars && \
    curl -L -o /opt/spark/jars/hadoop-aws.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    curl -L -o /opt/spark/jars/aws-java-sdk-bundle.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_BUNDLE_VERSION}.jar     

COPY activities-7.py /opt/spark/python/file-streaming.py
COPY activities-8.py /opt/spark/python/kafka-streaming.py

