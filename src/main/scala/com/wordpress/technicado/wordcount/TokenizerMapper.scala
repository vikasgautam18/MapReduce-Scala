package com.wordpress.technicado.wordcount

import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._

class TokenizerMapper
  extends Mapper[LongWritable, Text, Text, IntWritable] {

  override def map(key: LongWritable,
                   value: Text,
                   context: Mapper[LongWritable, Text, Text, IntWritable]#Context) {

    val words: Array[String] = value.toString.split("\\s+")

    words.foreach { word =>
      val key = new Text(word)
      val value = new IntWritable(1)
      context.write(key, value)
    }

  }
}