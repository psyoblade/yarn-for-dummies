## 얀 어플리케이션 개발

> 가장 단순한 헬로월드 appMaster 구현 부터 복잡한 애플리케이션까지 구현하는 것을 목적으로 합니다 
> [Hadoop Distributed Shell](https://github.com/psyoblade/hadoop/tree/aa74a303ed30a057893c6d2d9fb1e07e7d1f4a7d/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-applications/hadoop-yarn-applications-distributedshell/src/main/java/org/apache/hadoop/yarn/applications/distributedshell) 을 참고하여 개발합니다

### HelloWorld v1

> 아래와 같이 빌드 및 애플리케이션 실행합니다
```bash
# cat deploy.sh
docker-compose up -d

# build & deploy jar
sbin/deploy_yarn_app.sh hello-world-v1 true

# custom build & deploy
./gradlew clean build
docker cp build/libs/hello-world-v1.jar datanode:/
docker cp build/resources/main/log4j.properties datanode:/
docker-compose exec datanode hadoop jar ./hello-world-v1.jar .
```

### Debugging on http://resourcemanager:8088/cluster
```bash
# cat /etc/hosts or C:\Windows\System32\drivers\etc\hosts
127.0.0.1	nodemanager
127.0.0.1	resourcemanager
127.0.0.1	namenode
127.0.0.1	datanode
127.0.0.1	historyserver
```