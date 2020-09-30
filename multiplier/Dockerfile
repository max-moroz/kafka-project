FROM bellsoft/liberica-openjdk-alpine:14

MAINTAINER Max Moroz

# Make port 8085 available to the world outside this container
EXPOSE 8085

COPY ./target/multiplier-0.0.1-SNAPSHOT.jar /usr/app/

WORKDIR /usr/app

RUN sh -c 'touch multiplier-0.0.1-SNAPSHOT.jar'

ENTRYPOINT ["java","-jar","multiplier-0.0.1-SNAPSHOT.jar"]

