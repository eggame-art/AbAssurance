FROM eclipse-temurin:21-jdk-jammy AS build

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 unzip \
    && rm -rf /var/lib/apt/lists/*

ADD --checksum=sha256:ed9842e03b5369c3aa2383ac3c730f29193ac61ada6176678d37fecdf7098217 \
    https://deilink.fr/ruxie/Talaxie-9.0.2-linux-x86_64.zip /tmp/talaxie.zip
RUN unzip -q /tmp/talaxie.zip -d /opt \
    && mv /opt/Talaxie /opt/talaxie \
    && chmod +x /opt/talaxie/ruxie-server \
    && rm /tmp/talaxie.zip

ENV RUXIE_COMPONENTS_DIR=/opt/talaxie/codegen/talaxie/plugins/org.talend.designer.components.localprovider/components
COPY project/ /sources/project/
COPY tests/ /sources/tests/
COPY build.py /sources/build.py
RUN python3 /sources/build.py

FROM eclipse-temurin:21-jre-jammy AS runtime
WORKDIR /opt/job/ExtractPublish
COPY --from=build /output/ /opt/job/
USER 10001:10001
ENTRYPOINT ["java", "-Duser.timezone=UTC", "-Dfile.encoding=UTF-8", "-cp", "/opt/job/ExtractPublish:/opt/job/lib/*:/opt/job/ExtractPublish/extractpublish_0_1.jar", "abassurance.extractpublish_0_1.ExtractPublish", "--context=Default"]
