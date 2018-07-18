package com.wordpress.technicado.wordcount

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

object WordCount  {
  def main(args: Array[String]): Unit = {

    if(args.length != 3){
      println("usage: hadoop jar <Jar File Name> <fully qualified class path> <Input path> <Output Path>")
      System.exit(-1)
    }
    val conf = new Configuration
    val job = Job.getInstance(conf, "Word Count")
    job.setJarByClass(classOf[TokenizerMapper])

    FileInputFormat.addInputPath(job, new Path(args(1)))

    job.setMapperClass(classOf[TokenizerMapper])
    job.setReducerClass(classOf[IntSumReducer])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    FileOutputFormat.setOutputPath(job, new Path(args(2)))

    job.waitForCompletion(true)
  }
}