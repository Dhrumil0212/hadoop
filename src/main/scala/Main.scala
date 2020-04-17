

import org.apache.hadoop
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.conf.Configuration


object  Main extends App {

  val conf = new Configuration()

  conf.addResource(new Path("file:///home/bd-user/opt/hadoop-2.7.3/etc/cloudera/core-site.xml"))
  conf.addResource(new Path("file:///home/bd-user/opt/hadoop-2.7.3/etc/cloudera/hdfs-site.xml"))


  //  conf.addResource(new Path("file:///home/bd-user/opt/hadoop-2.7.3/etc/cloudera/core-site.xml"))
//  conf.addResource(new Path("file:///home/bd-user/opt/hadoop-2.7.3/etc/cloudera/hdfs-site.xml"))



  val fs: FileSystem = FileSystem.get(conf)
  val deletePaths = fs.globStatus(new Path("/user/fall2019/dhrumil")).map(_.getPath)
  val fileExists = fs.exists((new Path("/user/fall2019/bvbvbvbv")))
  val outputPath = new Path("/user/fall2019/dhrumil")
  if (fs.exists(new Path("/user/fall2019/dhrumil"))){
    println("deleting file:")


//
//fs.create(new Path("/user/fall2019/dhrumil2"))
//  fs.delete(new Path("/user/fall2019/dhrumil2"))
println("Directory deleted")

    fs.mkdirs(new Path("dhrumil24/stm"))
    fs.rename(new Path("dhrumil2/hi/stops.txt"),new Path("dhrumil2/hi/stops.csv"))

    println("Directory Created again!")

fs.copyFromLocalFile(new Path("/home/bd-user/Desktop/gtfs_stm/stops.txt"),new Path("dhrumil24/stm"))
//    fs.copyToLocalFile(new Path("dhrumil2/hi/stops.txt"),new Path("/home/bd-user/Desktop"))
    if (fileExists) {
      println("I found my folder!")
    } else {
      println("I failed in the previous practice!")
    }
  }

//   fs.removeAcl(new Path("dhrumil/stops.txt") )  //// Deletes the directory.


//    fs.delete(new Path("/user/fall2019/dhrumil"),true)  //// Deletes the directory.

println(fs.getUri)
  fs
    .listStatus(
      new Path("dhrumil/stm"))
    .map(_.getPath)
    .foreach(println)
  println("=============================================")
  fs
    .listStatus(
      new Path("/user/fall2019/"))
    .map(_.getPath)
    .foreach(println)
}

