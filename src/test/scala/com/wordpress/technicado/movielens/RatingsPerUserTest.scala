package com.wordpress.technicado.movielens

import org.apache.hadoop.io.{IntWritable, Text}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.FlatSpec
import org.mockito.Mockito._

import scala.collection.JavaConverters._

class RatingsPerUserTest extends FlatSpec with MockitoSugar{

  "MovieMapper" should "output user ids" in {

    val mapper = new MovieMapper
    val context = mock[mapper.Context]

    mapper.map(
      key = null,
      value = new Text("196\t242\t3\t881250949"),
      context
    )

    verify(context).write(new Text("196"), new IntWritable(1))
  }

  "NumRatingsReducer" should "count the number of ratings" in {

    val reducer = new NumRatingsReducer
    val context = mock[reducer.Context]

    reducer.reduce(
      key = new Text("196"),
      values = Seq(new IntWritable(1), new IntWritable(1), new IntWritable(1)).asJava,
      context = context
    )

    verify(context).write(new Text("196"), new IntWritable(3))
  }

}
