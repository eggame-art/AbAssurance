FROM apache/hop:2.20.0@sha256:8a23fd00099874e76fc1cbc29f7e7aecc68de95445c062ba6dcbb51e9b4c5866
USER root
RUN apk add --no-cache python3
ADD --checksum=sha256:f52e9335276cd3999c23bb5a364fa01a628f3468945444d03cb7f92271bca1da https://repo.maven.apache.org/maven2/com/oracle/database/jdbc/ojdbc11/23.9.0.25.07/ojdbc11-23.9.0.25.07.jar /opt/hop/lib/jdbc/ojdbc11.jar
COPY project /opt/hop/project
COPY run.py /opt/hop/run.py
COPY hop-config.json /opt/hop/config/hop-config.json
ENV HOP_CONFIG_FOLDER=/opt/hop/config
ENV HOP_OPTIONS="-Xmx512m -Djava.awt.headless=true -Duser.timezone=UTC -Dfile.encoding=UTF-8"
ENV HOP_COUNTS_DIR=/tmp/hop-counts
ENTRYPOINT ["python3", "/opt/hop/run.py"]
