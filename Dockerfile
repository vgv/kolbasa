FROM eclipse-temurin:17-jre

# OCI image labels (title, description, source, licenses, version, …) are set at release time by
# docker/metadata-action in .github/workflows/butcher-release.yml — its --label values override any
# LABEL here, so labels live there as the single source of truth rather than being duplicated.

WORKDIR /work

COPY build/libs/butcher.jar /app/butcher.jar

ENTRYPOINT ["java", "-jar", "/app/butcher.jar"]
