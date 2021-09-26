FROM openjdk:8-jdk-slim-bullseye
LABEL maintainer="Suhyuk Park <park.suhyuk@gmail.com>"

WORKDIR /app
ADD build/libs/yarn-for-dummies-1.0-SNAPSHOT.jar /app
ADD conf /app/conf
ADD src/main/resources/log4j.properties /app

ENTRYPOINT [ "java", "-jar", "/app/yarn-for-dummies-1.0-SNAPSHOT.jar" ]
CMD [ "" ]