FROM bellsoft/liberica-openjdk-alpine:14

MAINTAINER Max Moroz

# Make port 8084 available to the world outside this container
EXPOSE 8084

COPY ./target/adder-0.0.1-SNAPSHOT.jar /usr/app/

WORKDIR /usr/app

RUN sh -c 'touch adder-0.0.1-SNAPSHOT.jar'

ENTRYPOINT ["java","-jar","adder-0.0.1-SNAPSHOT.jar"]

