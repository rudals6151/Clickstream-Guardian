FROM apache/spark:3.5.0

USER root

# Install Python and pip
RUN apt-get update && apt-get install -y python3-pip && rm -rf /var/lib/apt/lists/*

# Install Python dependencies including PySpark
RUN pip3 install --no-cache-dir \
    pyspark==3.5.0 \
    kafka-python==2.0.2 \
    confluent-kafka==2.3.0 \
    apache-avro>=1.11.0 \
    psycopg2-binary==2.9.9 \
    boto3==1.34.0

# Install PostgreSQL JDBC driver and Hadoop AWS libraries
RUN curl -o /opt/spark/jars/postgresql-42.6.0.jar \
    https://jdbc.postgresql.org/download/postgresql-42.6.0.jar && \
    curl -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -o /opt/spark/jars/aws-java-sdk-bundle-1.12.367.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar

# Create spark configuration to disable Kerberos
RUN mkdir -p /opt/spark/conf && \
    echo "spark.hadoop.hadoop.security.authentication=simple" > /opt/spark/conf/spark-defaults.conf && \
    echo "spark.hadoop.hadoop.security.authorization=false" >> /opt/spark/conf/spark-defaults.conf

USER 185
