# syntax=docker/dockerfile:1
# Build the replayer with Gradle, then run it on a slim JRE.
FROM gradle:9.5.1-jdk21 AS build
WORKDIR /home/gradle/project
COPY settings.gradle build.gradle ./
COPY src ./src
RUN --mount=type=cache,target=/home/gradle/.gradle gradle --no-daemon installDist

FROM eclipse-temurin:21-jre
WORKDIR /opt/replayer
COPY --from=build /home/gradle/project/build/install/replayer/ ./
ENV DATA_DIR=/data
ENTRYPOINT ["/opt/replayer/bin/replayer"]
