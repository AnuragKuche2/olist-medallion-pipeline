# infra/eks/spark-docker/Dockerfile
# Custom Spark image with Delta Lake + project code
#
# Build: docker build -t olist-spark:3.5.0 .
# Push:  docker tag olist-spark:3.5.0 025078772864.dkr.ecr.us-west-2.amazonaws.com/olist-spark:3.5.0
#        docker push 025078772864.dkr.ecr.us-west-2.amazonaws.com/olist-spark:3.5.0

FROM apache/spark:3.5.0

USER root

# Install Python dependencies
COPY requirements.txt /opt/spark/work-dir/
RUN pip install --no-cache-dir -r /opt/spark/work-dir/requirements.txt

# Copy project code
COPY src/ /opt/spark/work-dir/src/

# Delta Lake JARs
RUN curl -L -o /opt/spark/jars/delta-spark_2.12-3.1.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar && \
    curl -L -o /opt/spark/jars/delta-storage-3.1.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar

# Hadoop AWS connector for S3
RUN curl -L -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

USER spark

WORKDIR /opt/spark/work-dir
