hdfs dfs -ls hdfs://hadoop-master:9000/
hdfs dfs -mkdir hdfs://hadoop-master:9000/data
hdfs dfs -put HTTP_20130313143750.dat hdfs://hadoop-master:9000/data/HTTP_20130313143750.dat
hdfs dfs -ls hdfs://hadoop-master:9000/data


create 'wlan_log', 'cf'

需要手动打成jar包执行，或者使用ant
