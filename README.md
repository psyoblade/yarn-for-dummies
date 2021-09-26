## 얀 어플리케이션 개발

> 가장 단순한 헬로월드 appMaster 구현 부터 복잡한 애플리케이션까지 구현하는 것을 목적으로 합니다 
> [Hadoop Distributed Shell](https://github.com/psyoblade/hadoop/tree/aa74a303ed30a057893c6d2d9fb1e07e7d1f4a7d/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-applications/hadoop-yarn-applications-distributedshell/src/main/java/org/apache/hadoop/yarn/applications/distributedshell) 을 참고하여 개발합니다

### HelloWorld v1

> 아래와 같이 빌드 및 애플리케이션 실행합니다
```bash
# cat deploy.sh
docker-compose up -d

./gradlew clean build
docker cp build/libs/yarn-for-dummies-v1.jar datanode:/
docker cp build/resources/main/log4j.properties datanode:/
docker-compose exec datanode hadoop jar ./yarn-for-dummies-v1.jar .
```