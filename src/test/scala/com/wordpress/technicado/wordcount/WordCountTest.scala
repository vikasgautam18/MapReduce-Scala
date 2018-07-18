package com.wordpress.technicado.wordcount

import org.apache.hadoop.io._
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.mockito.MockitoSugar

import scala.collection.JavaConverters._

class WordCountTest extends FlatSpec with MockitoSugar {

  "map" should "output the words split on spaces" in {
    val mapper = new TokenizerMapper
    val context = mock[mapper.Context]

    mapper.map(
      key = null,
      value = new Text("hello there hello"),
      context
    )

    verify(context, times(2)).write(new Text("hello"), new IntWritable(1))
    verify(context).write(new Text("there"), new IntWritable(1))
  }

  "reduce" should "output the count of each word" in {

    val reducer = new IntSumReducer
    val context = mock[reducer.Context]

    reducer.reduce(
      new Text("hello"),
      Seq(new IntWritable(1), new IntWritable(1)).asJava,
      context
    )

    verify(context).write(new Text("hello"), new IntWritable(2))
  }
}
