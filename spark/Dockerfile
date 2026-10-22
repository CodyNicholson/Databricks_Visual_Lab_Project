# The "cluster": a Spark 4.2 Connect server (the same client/server protocol Databricks Connect
# uses) with Delta Lake and the Unity Catalog connector on the classpath.
FROM apache/spark:4.2.0-scala2.13-java21-python3-ubuntu
ARG DELTA_VERSION=4.4.1
ARG UC_VERSION=0.7.0
USER root
# Python UDFs run in Python workers on this server: they need pandas/pyarrow here, and the
# client's Python minor version must match this one (3.10).
RUN python3 -m pip install --no-cache-dir pandas==2.3.3 pyarrow==25.0.1 numpy==2.2.6
# Resolve the connectors once at build time and put them on Spark's classpath, so the server
# starts offline and fast.
RUN mkdir -p /tmp/ivy && echo 'print("resolved")' > /tmp/noop.py \
 && /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/ivy \
      --packages io.delta:delta-spark_2.13:${DELTA_VERSION},io.delta:delta-connect-server_2.13:${DELTA_VERSION},io.unitycatalog:unitycatalog-spark_4.2_2.13:${UC_VERSION} \
      /tmp/noop.py \
 && ls /tmp/ivy/jars \
 && for j in /tmp/ivy/jars/*.jar; do b=$(basename "$j"); n=$(echo "$b" | sed -E 's/-[0-9][^-]*\.jar$//'); \
      if ls /opt/spark/jars/ | grep -q "^${n#*_}-[0-9]"; then echo "skip (bundled) $b"; else cp "$j" /opt/spark/jars/; fi; done \
 && rm -rf /tmp/ivy /tmp/noop.py
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY entrypoint.sh /opt/lab-entrypoint.sh
RUN chmod 755 /opt/lab-entrypoint.sh && mkdir -p /lakehouse && chown spark:spark /lakehouse && chmod 1777 /lakehouse
USER spark
EXPOSE 15002 4040
ENTRYPOINT ["/opt/lab-entrypoint.sh"]
