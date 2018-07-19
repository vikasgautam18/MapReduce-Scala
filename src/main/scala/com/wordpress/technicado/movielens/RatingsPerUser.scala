package com.wordpress.technicado.movielens

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

object RatingsPerUser  {
  def main(args: Array[String]): Unit = {

    if(args.length != 2){
      println("usage: hadoop jar <Jar File Name> <fully qualified class path> <Input path> <Output Path>")
      args.foreach(println)
      System.exit(-1)
    }
    val conf = new Configuration
    val job = Job.getInstance(conf, "Ratings per user")
    job.setJarByClass(classOf[MovieMapper])

    FileInputFormat.addInputPath(job, new Path(args(0)))

    job.setMapperClass(classOf[MovieMapper])
    job.setReducerClass(classOf[NumRatingsReducer])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    FileOutputFormat.setOutputPath(job, new Path(args(1)))

    job.waitForCompletion(true)
  }
}
