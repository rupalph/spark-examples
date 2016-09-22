/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


/** Computes an approximation to pi */
object WordCount {
  def remove(s: String): String = { s.filterNot(x => x == ',' || x == '!' || x == '.' || x == ''').replaceAll("-"," ") }
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val lines = sc.textFile(args(0))
    val pairs = lines.map(remove).flatMap ( s=>s.split(" ")).map (s=>(s,1))
    val count = pairs.reduceByKey((a,b) =>a+b)
    println("Word Count result " + count.collect().mkString(", "))
  }
}
// scalastyle:on println
