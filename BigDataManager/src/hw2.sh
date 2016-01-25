rm -f *.class *.jar
javac Hw2Part1.java
jar cfm Hw2Part1.jar Hw2Part1-manifest.txt Hw2Part1*.class
hadoop jar ./Hw2Part1.jar /hw2/input_0 /hw2/outputpp
hdfs dfs -cat '/hw2/outputpp/part-*'
