Spark Streaming with Real Time Data and Kafka

1. Download Kafka 3.5.1 from https://downloads.apache.org/kafka/
2. Start zookeeper by opening terminal and going to the directory where Kafka was installed and run

   bin/zookeeper-server-start.sh ../config/zookeeper.properties
   
3. Start Kafka server by opening a new terminal and going to the directory where Kafka was installed and run

   bin/kafka-server-start.sh ../config/server.properties
   
4. Create Kafka topics by opening a new terminal and going to the directory where Kafka was installed and run

   bin/kafka-topics.sh --create --topic reddit-comments --bootstrap-server localhost:9092
   

   bin/kafka-topics.sh --create --topic word-counts --bootstrap-server localhost:9092
   
This will create two topics reddit-comments and word-counts
5. To get messages in topic, open a new terminal and go to the directory where Kafka was installed and run
    
    bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic reddit-comments --from-beginning
    
6. To send messages, open a new terminal and go to the directory where Kafka was installed and run

   bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic reddit-comments
   
7. Download elastic search from https://www.elastic.co/downloads/elasticsearch
8. Download kibana from https://www.elastic.co/downloads/kibana
9. Download log stash from https://www.elastic.co/downloads/logstash
10.  Go to directory where elastic search was installed and Disable SSL in config/elasticsearch.yml by changing below properties

xpack.security.enabled: false
xpack.security.http.ssl:
    enabled: false
    
11. Open new terminal and go to directory where elasticsearch was installed and run

    bin/elasticsearch
    
12. Go to directory where kibana was installed and go to config/kibana.yml and change below properties

    elasticsearch.hosts: [“http://192.168.1.112:9200”]
    
13. Open new terminal and go to directory where kibana was installed and run

    bin/kibana
    
14. Go to folder where logstash was installed and upload logstash.conf mentioned in report.
15. Open new terminal and go to directory where logstash was installed and run

    bin/logstash -f logstash.conf
    
16. Install pyspark of latest version

    pip3 install pyspark==3.4.1
    
17. Download spark from https://spark.apache.org/downloads.html
18. Open folder where spark was installed and upload producer.py and consumer.py in that folder.
19. Create a checkpoint directory in same location.
20. Open a new terminal and go to directory where spark was installed and run

    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 --conf spark.sql.streaming.forceDeleteTempCheckpointLocation=true consumer.py /tmp/checkpoint localhost:9092 reddit-comments word-counts
    
21. Open a new terminal and go to directory where spark was installed and run

    python3 -u producer.py reddit-comments localhost:9092
    
22. Open new tab in chrome and open kibana by typing localhost:5601/
23. Go to Management and Click on Data views
24. Create a Data view by giving a name and put index pattern as word-counts* and time stamp as needed and create data view
25. Go to Dashboard and create visualization.
26. Select index created and select words.keyword  field in horizontal axis.
27. Adjust number of values to 10 to get top 10 named entities.
28. Select count field in vertical axis and aggregation function as sum
29. Visualizations appear as data is streamed and you can change time in dashboard to see visualizations at different times.
30. Save Visualizations.
