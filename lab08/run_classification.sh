bin/hdfs dfs -rm -r -f -skipTrash output
bin/hdfs dfs -rm -r -f -skipTrash output1
bin/hdfs dfs -rm -r -f -skipTrash output2
bin/hdfs dfs -rm -r -f -skipTrash output3

bin/hadoop jar Classification.jar input output

bin/hdfs dfs -cat output3/*
