# syntax=docker/dockerfile:1
# Build + unit-test the Streams app, then run it on a slim JRE.
FROM gradle:9.5.1-jdk21 AS build
WORKDIR /home/gradle/project
COPY settings.gradle build.gradle ./
COPY src ./src
RUN --mount=type=cache,target=/home/gradle/.gradle gradle --no-daemon test installDist

FROM eclipse-temurin:21-jre
WORKDIR /opt/streams-app
COPY --from=build /home/gradle/project/build/install/streams-app/ ./
ENV STATE_DIR=/tmp/kafka-streams
ENTRYPOINT ["/opt/streams-app/bin/streams-app"]
