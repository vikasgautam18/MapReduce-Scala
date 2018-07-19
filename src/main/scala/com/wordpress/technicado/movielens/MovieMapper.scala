package com.wordpress.technicado.movielens

import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

class MovieMapper extends Mapper[LongWritable, Text, Text, IntWritable] {

  override def map(key: LongWritable, value: Text,
                   context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {

    val splitArr: Array[String] = value.toString.split("\t")
    val userId = new Text(splitArr(0))
    val rating = new IntWritable(1)

    context.write(userId, rating)
  }
}
