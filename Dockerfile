FROM maven:3-jdk-8-slim

ARG DEBIAN_FRONTEND=noninteractive

ENV MAVEN_CONFIG="/var/maven/.m2"
ENV HOME="/var/maven"
ENV MAVEN_HOME="/var/maven"
ENV MAVEN_OPTS="-Duser.home=/var/maven"

ADD target/docker-build/.m2 /var/maven/.m2

WORKDIR /data

ENTRYPOINT ["mvn"]