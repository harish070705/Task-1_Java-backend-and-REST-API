# Stage 1: Build the application
FROM maven:3.9-eclipse-temurin-17-alpine AS build
WORKDIR /app
COPY pom.xml .
COPY .mvn .mvn
COPY src src
# This runs the "code build" step using Maven
RUN mvn package -DskipTests

# Stage 2: Create the final, small image
FROM eclipse-temurin:17-jre-alpine
WORKDIR /opt/app
# Copy the built jar from the 'build' stage
COPY --from=build /app/target/*.jar app.jar
# Set the port the app runs on
EXPOSE 9090
# Command to run the application
ENTRYPOINT ["java", "-jar", "app.jar"]