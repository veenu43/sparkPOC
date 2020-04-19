mvn exec:java -Dexec.mainClass=com.journaldev.sparkdemo.WordCounter -Dexec.args="input.txt"


spark-submit --class "spark.WordCounterPlural" --master "local[*]" "D:\Workspaces\spark-poc\target\spark-poc-1.0-SNAPSHOT.jar"