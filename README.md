# Apache Spark

Spark is a fast and general cluster computing system for Big Data. It provides
high-level APIs in Scala, Java, and Python, and an optimized engine that
supports general computation graphs for data analysis. It also supports a
rich set of higher-level tools including Spark SQL for SQL and structured
data processing, MLlib for machine learning, GraphX for graph processing,
and Spark Streaming for stream processing.

<http://spark.apache.org/>


## Online Documentation

You can find the latest Spark documentation, including a programming
guide, on the [project web page](http://spark.apache.org/documentation.html)
and [project wiki](https://cwiki.apache.org/confluence/display/SPARK).
This README file only contains basic setup instructions.

## Building Spark

Spark is built using [Apache Maven](http://maven.apache.org/).
To build Spark and its example programs, run:

    mvn -DskipTests clean package

(You do not need to do this if you downloaded a pre-built package.)
More detailed documentation is available from the project site, at
["Building Spark with Maven"](http://spark.apache.org/docs/latest/building-with-maven.html).

## Interactive Scala Shell

The easiest way to start using Spark is through the Scala shell:

    ./bin/spark-shell

Try the following command, which should return 1000:

    scala> sc.parallelize(1 to 1000).count()

## Interactive Python Shell

Alternatively, if you prefer Python, you can use the Python shell:

    ./bin/pyspark
    
And run the following command, which should also return 1000:

    >>> sc.parallelize(range(1000)).count()

## Example Programs

Spark also comes with several sample programs in the `examples` directory.
To run one of them, use `./bin/run-example <class> [params]`. For example:

    ./bin/run-example SparkPi

will run the Pi example locally.

You can set the MASTER environment variable when running examples to submit
examples to a cluster. This can be a mesos:// or spark:// URL, 
"yarn-cluster" or "yarn-client" to run on YARN, and "local" to run 
locally with one thread, or "local[N]" to run locally with N threads. You 
can also use an abbreviated class name if the class is in the `examples`
package. For instance:

    MASTER=spark://host:7077 ./bin/run-example SparkPi

Many of the example programs print usage help if no params are given.

## Running Tests

Testing first requires [building Spark](#building-spark). Once Spark is built, tests
can be run using:

    ./dev/run-tests

Please see the guidance on how to 
[run all automated tests](https://cwiki.apache.org/confluence/display/SPARK/Contributing+to+Spark#ContributingtoSpark-AutomatedTesting).

## A Note About Hadoop Versions

Spark uses the Hadoop core library to talk to HDFS and other Hadoop-supported
storage systems. Because the protocols have changed in different versions of
Hadoop, you must build Spark against the same version that your cluster runs.

Please refer to the build documentation at
["Specifying the Hadoop Version"](http://spark.apache.org/docs/latest/building-with-maven.html#specifying-the-hadoop-version)
for detailed guidance on building for a particular distribution of Hadoop, including
building for particular Hive and Hive Thriftserver distributions. See also
["Third Party Hadoop Distributions"](http://spark.apache.org/docs/latest/hadoop-third-party-distributions.html)
for guidance on building a Spark application that works with a particular
distribution.

## Configuration

Please refer to the [Configuration guide](http://spark.apache.org/docs/latest/configuration.html)
in the online documentation for an overview on how to configure Spark.

# Instructions to Run "BenchmarkTimestamp"

There are two parts to this benchmark as well. In this benchmark, the generator sends timestamps one at a time in plain text, and for every timestamp the application processes, it takes the current timestamp, compute the differences, and prints all 3 numbers to screen.

1. **DataGeneratorTimestamp**: This generates data for the benchmarking. It takes as input the following:
    - The port that it listens on for incoming connections
    - The byte rate at which it sends data (timestamps in plain text) to any client that connects to it

2. **BenchmarkTimpstamp**: This consumes the generated data and after every batch interval (say, 1 second), and prints the number of records received in the batch interval as well as some example timestamp differences. It takes as input the following:
    - The number streams it will create. This is the number of parallel data generators it is going to connect to. Typically this is the number of workers on which the benchmark is being run on (or 1 if it is being run on a local machine)
    - The host and port of the data generator it is going to connect to
    - The batch interval in milliseconds (1000 is typical)
    - (Optional) The block interval in milliseconds (see Spark Streaming Programming Guide for more details)

## Running on a Local Machine or Cluster
The process is similar to running "Benchmark". On terminal 1, run DataGeneratorTimestamp.

    > sbt/sbt "run-main DataGeneratorTimestamp 9999 1000000"

On terminal 2, run BenchmarkTimestamp

    > sbt/sbt "run-main BenchmarkTimestamp 1 localhost 9999 1000 100" 2>&1 | grep "Total delay\|records\|^[0-9]\+ [0-9]\+ [0-9]\+$"

