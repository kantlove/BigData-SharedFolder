# copy the original dataset file
bin/hdfs dfs -rm -r -f dataset
bin/hdfs dfs -mkdir dataset
bin/hdfs dfs -put input/k-means/dataset.txt dataset

# copy the original center file
bin/hdfs dfs -rm -r -f center
bin/hdfs dfs -mkdir center
bin/hdfs dfs -put input/k-means/centers.txt center

# copy the original input file
bin/hdfs dfs -rm -r -f input
bin/hdfs dfs -mkdir input
bin/hdfs dfs -put input/k-means/kmeans_input.txt input

# run the program
bin/hdfs dfs -rm -r -f output
bin/hadoop jar KMeans.jar 2 dataset center input output 10
echo "---------------------"
echo "New centers"
echo "---------------------"
bin/hdfs dfs -cat center/*
echo "---------------------"
echo "Final output"
echo "---------------------"
bin/hdfs dfs -cat output/*
