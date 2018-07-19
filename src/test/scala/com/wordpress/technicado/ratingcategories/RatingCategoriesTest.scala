package com.wordpress.technicado.ratingcategories

import org.apache.hadoop.io.{IntWritable, Text}
import org.scalatest.FlatSpec
import org.scalatest.mockito.MockitoSugar
import org.mockito.Mockito._
import scala.collection.JavaConverters._

class RatingCategoriesTest extends FlatSpec with MockitoSugar{


  "mapper" should "pick ratings from each row" in {

    val mapper = new RatingMapper
    val mapContext = mock[mapper.Context]

    mapper.map(
      key = null,
      value = new Text("196\t242\t3\t881250949"),
      mapContext
    )

    verify(mapContext).write(new Text("3"), new IntWritable(1))
  }

  "reducer" should "count ratings" in {

    val reducer = new CountRatingsReducer
    val reduceContext = mock[reducer.Context]

    reducer.reduce(
      key = new Text("3"),
      values = Seq(new IntWritable(1), new IntWritable(1), new IntWritable(1), new IntWritable(1)).asJava,
      reduceContext
    )

    verify(reduceContext).write(new Text("3"), new IntWritable(4))

  }

}
