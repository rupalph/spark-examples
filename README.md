#spark-examples

1. WordCount
Counts Words after removing puncuations from the file.

To build, run:
sbt package

To run:
./bin/spark-submit --class "WordCount" --master local[4] spark-examples/target/scala-2.11/sparkme-project_2.11-0.1-SNAPSHOT.jar <path to textfile>

#install sbt
brew install sbt

#install spark
--download from http://spark.apache.org/downloads.html
--unzip/untar in a directory.
