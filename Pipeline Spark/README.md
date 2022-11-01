# Pipeline de dados em spark

- Exemplo de código de pipeline de dados usando Kafka, Spark e Airflow configuração em docker

## Pré-requisito

### Configuração do Kafka

- Instale o Kafka (usei o Kafka 2.6.0 para este exemplo)
- Inicie o Zookeeper e o servidor kafka
- Crie um tópico chamado "dados do paciente"

```
  kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic topic-mane
```

### Configuração do Spark

- Instale o Spark na imagem do docker

### Configuração do AWS S3

- Conta AWS e bucket S3
- Chave de acesso e chave secreta para a conta da AWS

### Configuração do fluxo de ar

- Instalar fluxo de ar na imagem do docker

## Airflow para acionar o Spark Job que busca dados do tópico Kafka

1. Ingerir dados para o tópico kafka

```
kafka-console-producer --broker-list localhost:9092 --topic patient-data

Digite a mensagem : Esta é uma mensagem de teste kafka
```

2. Altere o trabalho e teste do Spark

- Altere as configurações do mestre do Spark em ConsumerSpark.py com base na instalação do Spark.
- Execute o trabalho de ignição (ConsumerSpark.py) manualmente

```
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 ConsumerSpark.py
```

3. Deve exibir a contagem de mensagens no tópico Kafka

4. Use o Apache Airflow para automatizar o agendamento do trabalho do Spark

- Atualize Dag.py com base na localização do arquivo
- Coloque Dag.py na pasta "dags" no diretório inicial do Airflow

5. Iniciar fluxo de ar

- airflow initdb
- servidor web do airflow

6. Iniciar agendador de fluxo de ar

- programador de fluxo de ar

7. Vá para a interface do Airflow e acione "spark_job_dag"

## Spark Job que busca dados do tópico Kafka e salva isso no AWS S3

1. Ingerir dados para o tópico kafka

```
 kafka-console-producer --broker-list localhost:9092 --topic topic-name
```

2. Altere o trabalho e teste do Spark

- Altere Data Pipeline.py para incluir ACCESS_KEY, SECRET_KEY, S3_BUCKET corretos
- Iniciar Spark: _./start-all.sh_
- Execute o trabalho de ignição (Data Pipeline.py) manualmente

```
  /usr/local/Cellar/apache-spark/3.0.1/libexec/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,com .amazonaws:aws-java-sdk:1.11.633,org.apache.hadoop:hadoop-aws:3.2.0 /Users/philips/Documents/Study_code/Spark/spark-samples/spark_kafka_s3.py
```

- o comando pode mudar com base na estrutura da pasta, também as versões aws-java-sdk e hadoop-aws podem ser diferentes
- Se for bem-sucedido, salvará os dados do parquet no bucket do S3
- Use o Apache Airflow para automatizar o agendamento do trabalho do Spark
- Atualize save_data_S3.py com base na localização do arquivo
- Coloque save_data_S3.py na pasta "dags" no diretório inicial do Airflow

3. Iniciar fluxo de ar

- airflow initdb
- servidor web de airflow

4. Iniciar agendador de airflow

- programador de airflow

5. Vá para a interface do Airflow e acione "S3_data_save_dag"

## Referências:

[Spark Doc](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
