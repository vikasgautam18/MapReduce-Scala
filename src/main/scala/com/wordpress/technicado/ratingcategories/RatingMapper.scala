package com.wordpress.technicado.ratingcategories

import org.apache.hadoop.io
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

class RatingMapper extends Mapper[LongWritable, Text, Text, IntWritable]{

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context) = {
    val splitArr: Array[String] = value.toString.split("\t")
    val rating = splitArr(2)

    context.write(new Text(rating), new io.IntWritable(1))
  }
}
