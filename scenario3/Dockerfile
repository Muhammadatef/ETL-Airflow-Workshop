FROM bde2020/hadoop-base:2.0.0-hadoop2.7.4-java8

# Set versions
ARG HIVE_VERSION=2.3.2
ENV HIVE_HOME=/opt/hive
ENV PATH=$HIVE_HOME/bin:$PATH

WORKDIR /opt

# Fix repository sources
RUN echo "deb http://archive.debian.org/debian stretch main" > /etc/apt/sources.list && \
    echo "deb http://archive.debian.org/debian-security stretch/updates main" >> /etc/apt/sources.list

# Install Hive with dependencies
RUN apt-get update && \
    apt-get install -y --allow-unauthenticated wget procps && \
    wget https://archive.apache.org/dist/hive/hive-$HIVE_VERSION/apache-hive-$HIVE_VERSION-bin.tar.gz && \
    tar -xzvf apache-hive-$HIVE_VERSION-bin.tar.gz && \
    mv apache-hive-$HIVE_VERSION-bin hive && \
    mkdir -p $HIVE_HOME/lib && \
    wget https://jdbc.postgresql.org/download/postgresql-42.2.24.jar -O $HIVE_HOME/lib/postgresql-jdbc.jar && \
    rm apache-hive-$HIVE_VERSION-bin.tar.gz && \
    apt-get purge -y wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy configurations
COPY hive-config/ $HIVE_HOME/conf/
COPY entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/entrypoint.sh

EXPOSE 9083
ENTRYPOINT ["entrypoint.sh"]