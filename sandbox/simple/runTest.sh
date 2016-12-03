#!/bin/bash
set -x
../../bin/spark-submit --class="SimpleApp" --master local[1] target/scala-2.11/simple-project_2.11-1.0.jar
