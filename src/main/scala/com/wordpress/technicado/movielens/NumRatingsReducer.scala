package com.wordpress.technicado.movielens

import java.lang.Iterable

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

import scala.collection.JavaConverters._

class NumRatingsReducer extends Reducer[Text, IntWritable, Text, IntWritable]{

  override def reduce(key: Text, values: Iterable[IntWritable],
                      context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {

    val count: Int = values.asScala.map(_.get).reduce((a, b) => a + b)
    context.write(key, new IntWritable(count))

  }
}
