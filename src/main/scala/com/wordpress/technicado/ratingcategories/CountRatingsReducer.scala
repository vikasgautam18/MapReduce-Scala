package com.wordpress.technicado.ratingcategories

import java.lang.Iterable

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import scala.collection.JavaConverters._

class CountRatingsReducer extends Reducer[Text, IntWritable, Text, IntWritable]{

  override def reduce(key: Text, values: Iterable[IntWritable],
                      context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {

    val sum = values.asScala.map(_.get).reduce((a,b) => a + b)
    context.write(key, new IntWritable(sum))
  }
}
