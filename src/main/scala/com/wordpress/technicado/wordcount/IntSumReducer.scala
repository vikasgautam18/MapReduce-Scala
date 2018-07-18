package com.wordpress.technicado.wordcount

import java.lang.Iterable

import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._

import scala.collection.JavaConverters._

class IntSumReducer
  extends Reducer[Text, IntWritable, Text, IntWritable] {

  override def reduce(key: Text,
                      values: Iterable[IntWritable],
                      context: Reducer[Text, IntWritable, Text, IntWritable]#Context) {

    val sum: Int = values
      .asScala.map(_.get).reduce((a, b) => a + b)

    context.write(key, new IntWritable(sum))
  }
}