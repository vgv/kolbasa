FROM eclipse-temurin:17-jre

LABEL org.opencontainers.image.source=https://github.com/vgv/kolbasa
LABEL org.opencontainers.image.title=butcher
LABEL org.opencontainers.image.description="CLI for managing a kolbasa cluster"

WORKDIR /work

COPY build/libs/butcher.jar /app/butcher.jar

ENTRYPOINT ["java", "-jar", "/app/butcher.jar"]
