FROM bellsoft/liberica-openjdk-alpine:14

MAINTAINER Max Moroz

# Make port 8090 available to the world outside this container
EXPOSE 8090

COPY ./target/generator-0.0.1-SNAPSHOT.jar /usr/app/

WORKDIR /usr/app

RUN sh -c 'touch generator-0.0.1-SNAPSHOT.jar'

ENTRYPOINT ["java","-jar","generator-0.0.1-SNAPSHOT.jar"]
