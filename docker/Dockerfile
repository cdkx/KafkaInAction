############################
# Build stage
############################
FROM maven:3.9.9-eclipse-temurin-17 AS build

ARG SERVICE=orders-service

WORKDIR /build

COPY pom.xml ./
COPY common/pom.xml common/
COPY orders-service/pom.xml orders-service/
COPY payment-service/pom.xml payment-service/
COPY shipping-service/pom.xml shipping-service/
COPY notifications-service/pom.xml notifications-service/

RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -pl ${SERVICE} -am dependency:go-offline -DskipTests || true

COPY . .

RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -pl ${SERVICE} -am package -DskipTests

RUN JAR_PATH=$(find /build/${SERVICE}/target -maxdepth 1 -name "*.jar" ! -name "*.original" ! -name "*-plain.jar" | head -n 1) \
    && cp "$JAR_PATH" /build/app.jar


############################
# Runtime stage
############################
FROM eclipse-temurin:17-jre-alpine AS runtime

WORKDIR /app

RUN addgroup -S app && adduser -S app -G app

COPY --from=build /build/app.jar /app/app.jar

RUN chown -R app:app /app
USER app

ENV JAVA_TOOL_OPTIONS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0"

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "/app/app.jar"]