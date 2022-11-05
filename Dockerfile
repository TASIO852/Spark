## construir nossa imagem em cima da versão mais recente do Debian
FROM debian:latest
LABEL maintainer="tasiosantos"
## diretório inicial
WORKDIR /home/timedados/Documents/Cloud/
## instale os pacotes necessários
RUN apt-get update && apt-get install -y curl vim wget software-properties-common ssh net-tools wamerican wamerican-insane wbrazilian wdutch wbritish-large
RUN apt-get install -y python3 python3-pip python3-numpy python3-matplotlib python3-scipy python3-pandas python3-simpy
## definir python3 como padrão 
RUN update-alternatives --install "/usr/bin/python" "python" "$(which python3)" 1
## mova os arquivos da pasta de downloads no host para /home no container 
## comente esta linha se preferir baixar durante o processo de compilação
COPY downloads/* /home/timedados/Documents/Cloud/
## descomente as próximas duas linhas se preferir baixar durante o processo de compilação
RUN curl -L -b "oraclelicense=a" -O http://download.oracle.com/otn-pub/java/jdk/8u191-b12/2787e4a523244c269598db4e85c51e0c/jdk-8u191-linux-x64.tar.gz
RUN curl -L -O https://www-eu.apache.org/dist/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3.0.3.tgz
## configure java jdk 8
RUN mkdir -p /usr/local/oracle-java-8
RUN tar -zxf jdk-8u191-linux-x64.tar.gz -C /usr/local/oracle-java-8/
RUN rm  jdk-8u191-linux-x64.tar.gz
RUN update-alternatives --install "/usr/bin/java" "java" "/usr/local/oracle-java-8/jdk1.8.0_191/bin/java" 1
RUN update-alternatives --install "/usr/bin/javac" "javac" "/usr/local/oracle-java-8/jdk1.8.0_191/bin/javac" 1
RUN update-alternatives --install "/usr/bin/javaws" "javaws" "/usr/local/oracle-java-8/jdk1.8.0_191/bin/javaws" 1
ENV JAVA_HOME="/usr/local/oracle-java-8/jdk1.8.0_191"
## configure spark
RUN mkdir -p /usr/local/spark-3.3.0
RUN tar -zxf spark-3.3.0-bin-hadoop3.0.3.tgz -C /usr/local/spark-3.3.0/
RUN rm spark-3.3.0-bin-hadoop3.0.3.tgz
RUN update-alternatives --install "/usr/sbin/start-master" "start-master" "/usr/local/spark-3.3.0/spark-3.3.0-bin-hadoop3.0.3/sbin/start-master.sh" 1
RUN update-alternatives --install "/usr/sbin/start-slave" "start-slave" "/usr/local/spark-3.3.0/spark-3.3.0-bin-hadoop3.0.3/sbin/start-slave.sh" 1
RUN update-alternatives --install "/usr/sbin/start-slaves" "start-slaves" "/usr/local/spark-3.3.0/spark-3.3.0-bin-hadoop3.0.3/sbin/start-slaves.sh" 1
RUN update-alternatives --install "/usr/sbin/start-all" "start-all" "/usr/local/spark-3.3.0/spark-3.3.0-bin-hadoop3.0.3/sbin/start-all.sh" 1
RUN update-alternatives --install "/usr/sbin/stop-all" "stop-all" "/usr/local/spark-3.3.0/spark-3.3.0-bin-hadoop3.0.3/sbin/stop-all.sh" 1
RUN update-alternatives --install "/usr/sbin/stop-master" "stop-master" "/usr/local/spark-3.3.0/spark-3.3.0-bin-hadoop3.0.3/sbin/stop-master.sh" 1
RUN update-alternatives --install "/usr/sbin/stop-slaves" "stop-slaves" "/usr/local/spark-3.3.0/spark-3.3.0-bin-hadoop3.0.3/sbin/stop-slaves.sh" 1
RUN update-alternatives --install "/usr/sbin/stop-slave" "stop-slave" "/usr/local/spark-3.3.0/spark-3.3.0-bin-hadoop3.0.3/sbin/stop-slave.sh" 1
RUN update-alternatives --install "/usr/sbin/spark-daemon.sh" "spark-daemon.sh" "/usr/local/spark-3.3.0/spark-3.3.0-bin-hadoop3.0.3/sbin/spark-daemon.sh" 1
RUN update-alternatives --install "/usr/sbin/spark-config.sh" "spark-config.sh" "/usr/local/spark-3.3.0/spark-3.3.0-bin-hadoop3.0.3/sbin/spark-config.sh" 1
RUN update-alternatives --install "/usr/bin/spark-shell" "spark-shell" "/usr/local/spark-3.3.0/spark-3.3.0-bin-hadoop3.0.3/bin/spark-shell" 1
RUN update-alternatives --install "/usr/bin/spark-class" "spark-class" "/usr/local/spark-3.3.0/spark-3.3.0-bin-hadoop3.0.3/bin/spark-class" 1
RUN update-alternatives --install "/usr/bin/spark-sql" "spark-sql" "/usr/local/spark-3.3.0/spark-3.3.0-bin-hadoop3.0.3/bin/spark-sql" 1
RUN update-alternatives --install "/usr/bin/spark-submit" "spark-submit" "/usr/local/spark-3.3.0/spark-3.3.0-bin-hadoop3.0.3/bin/spark-submit" 1
RUN update-alternatives --install "/usr/bin/pyspark" "pyspark" "/usr/local/spark-3.3.0/spark-3.3.0-bin-hadoop3.0.3/bin/pyspark" 1
RUN update-alternatives --install "/usr/bin/load-spark-env.sh" "load-spark-env.sh" "/usr/local/spark-3.3.0/spark-3.3.0-bin-hadoop3.0.3/bin/load-spark-env.sh" 1
ENV SPARK_HOME="/usr/local/spark-3.3.0/spark-3.3.0-bin-hadoop3.0.3"

##Configure kafka spark python dependencies

## expose for ssh
EXPOSE 22
## expose for spark use
EXPOSE 7000-8000
## expose for master webui
EXPOSE 8080
## expose for slave webui
EXPOSE 8081