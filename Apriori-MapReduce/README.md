The algorithm can be ran on distributed setting such as Michigan Cavium Thunderx Cluster.
However, the algorithm does not work for itemsets containing more than 2 elements. (i.e. k >= 3)
To run, use this command.
$hadoop -jar build/libs/FrequentItemsetsHadoop.jar -k <output itemset size> -s < minimum support threshold> --input_path <HDFS_INPUT_FILE> --output_path <HDFS_OUTPUT_LOCATION>