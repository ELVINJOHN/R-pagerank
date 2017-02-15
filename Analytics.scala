// scalastyle:off println
//package org.apache.spark.examples.graphx

import scala.collection.mutable
import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.graphx.PartitionStrategy._

/**
 * Driver program for running graph algorithms.
 */
object Analytics extends Logging {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println(
        "Usage: Analytics <taskType> <file> --numEPart=<num_edge_partitions> [other options]")
      System.err.println("Supported 'taskType' as follows:")
      System.err.println("  pagerank    Compute PageRank")
      System.err.println("  cc          Compute the connected components of vertices")
      System.err.println("  triangles   Count the number of triangles")
      System.exit(1)
    }

    val taskType = args(0)
    val fname = args(1)//"hdfs://master:9000/user/elvin/amazon.txt"	//val fname1 = "hdfs://master:9000/user/elvin/live.txt"
	val outfname = args(2)
  val optionsList = args.drop(3).map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => (opt -> v)
        case _ => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    } 
    val options = mutable.Map(optionsList: _*)
	

    val conf = new SparkConf().setMaster("spark://172.1.60.11:7077")
    GraphXUtils.registerKryoClasses(conf)

    val numEPart = options.remove("numEPart").map(_.toInt).getOrElse {
      println("Set the number of edge partitions using --numEPart.")
      sys.exit(1)
    }
    val partitionStrategy: Option[PartitionStrategy] = options.remove("partStrategy")
      .map(PartitionStrategy.fromString(_))
    val edgeStorageLevel = options.remove("edgeStorageLevel")
      .map(StorageLevel.fromString(_)).getOrElse(StorageLevel.MEMORY_ONLY)
    val vertexStorageLevel = options.remove("vertexStorageLevel")
      .map(StorageLevel.fromString(_)).getOrElse(StorageLevel.MEMORY_ONLY)

    taskType match {
      case "pagerank" =>
        val tol = options.remove("tol").map(_.toFloat).getOrElse(0.001F)
        val outFname = options.remove("output").getOrElse("")
        val numIterOpt = options.remove("numIter").map(_.toInt)

        options.foreach {
          case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
        }

        println("======================================")
        println("|             PageRank               |")
        println("======================================")

        val sc = new SparkContext(conf.setAppName("PageRank GraphX (" + fname + ")").setMaster("spark://172.1.60.11:7077"))

        val unpartitionedGraph = GraphLoader.edgeListFile(sc, fname,
          numEdgePartitions = numEPart,
          edgeStorageLevel = edgeStorageLevel,
          vertexStorageLevel = vertexStorageLevel).cache()
        val graph = partitionStrategy.foldLeft(unpartitionedGraph)(_.partitionBy(_))

        println("GRAPHX: Number of vertices " + graph.vertices.count)
        println("GRAPHX: Number of edges " + graph.edges.count)

        val pr = (numIterOpt match {
          case Some(numIter) => PageRank.run(graph, numIter)
          case None => PageRank.runUntilConvergence(graph, tol)
        }).vertices.cache()

        println("GRAPHX: Total rank: " + pr.map(_._2).reduce(_ + _))
pr.map { case (id, r) => id + "\t" + r }.saveAsTextFile(outFname)
        if (!outFname.isEmpty) {
          logWarning("Saving pageranks of pages to " + outFname)
                  }

        sc.stop()

      case "cc" =>
        options.foreach {
          case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
        }

        println("======================================")
        println("|      Connected Components          |")
        println("======================================")

        val sc = new SparkContext(conf.setAppName("ConnectedComponents(" + fname + ")").setMaster("spark://172.1.60.11:7077"))
        val unpartitionedGraph = GraphLoader.edgeListFile(sc, fname,
          numEdgePartitions = numEPart,
          edgeStorageLevel = edgeStorageLevel,
          vertexStorageLevel = vertexStorageLevel).cache()
        val graph = partitionStrategy.foldLeft(unpartitionedGraph)(_.partitionBy(_))

        //val cc = ConnectedComponents.run(graph).vertices
	//val scc = stronglyConnectedComponents.run(graph).vertices
	//graph.stronglyConnectedComponents(1).vertices.saveAsTextFile(outfname)
        println("======================================")
        println("|      Connected Components          |")
        println("======================================")
/*	//println("Components: " + cc.vertices.map(_._2).collect.distinct.size)
        //val ccf = cc.vertices.map(_._2).collect()
	//val pointsInCluster = cc.join(vertices)((id, cc, point) => (cc, point))
	//println("Components: " + cc.vertices.map(_._2).collect())
	//pointsInCluster.saveAsTextFile("/home/elvin/Analytics/op")
	//cc.vertices.map(_._2).saveAsTextFile("hdfs://master:9000/user/elvin/ccamazon1")
	//cc.vertices.map(_._2).distinct.saveAsTextFile(outfname)*/
	//val graph = GraphLoader.edgeListFile(sc, fname)
	// Find the connected components
	val cc = graph.connectedComponents().vertices
	cc.saveAsTextFile(outfname)
	println("======================================")
        println("|      Connected Components          |")
        println("======================================")
	println("Stopping Spark Context")
        sc.stop()


	case "scc" =>
        options.foreach {
          case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
        }

        println("======================================")
        println("|   strongly Connected Components    |")
        println("======================================")

        val sc = new SparkContext(conf.setAppName("stronglyConnectedComponents(" + fname + ")").setMaster("spark://172.1.60.11:7077"))
        val graph = GraphLoader.edgeListFile(sc, fname)
	val sccGraph = graph.stronglyConnectedComponents(2).vertices.saveAsTextFile(outfname)
	println("======================================")
        println("|      Connected Components          |")
        println("======================================")
/*	//println("Components: " + cc.vertices.map(_._2).collect.distinct.size)
        //val ccf = cc.vertices.map(_._2).collect()
	//val pointsInCluster = cc.join(vertices)((id, cc, point) => (cc, point))
	//println("Components: " + cc.vertices.map(_._2).collect())
	//pointsInCluster.saveAsTextFile("/home/elvin/Analytics/op")
	//cc.vertices.map(_._2).saveAsTextFile("hdfs://master:9000/user/elvin/ccamazon1")
	//cc.vertices.map(_._2).distinct.saveAsTextFile(outfname)*/
	//val graph = GraphLoader.edgeListFile(sc, fname)
	// Find the connected components
	//val cc = graph.connectedComponents().vertices
	//cc.saveAsTextFile(outfname)
	println("======================================")
        println("|      Connected Components          |")
        println("======================================")
	println("Stopping Spark Context")
        sc.stop()

      case "triangles" =>
        options.foreach {
          case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
        }

        println("======================================")
        println("|      Triangle Count                |")
        println("======================================")

        val sc = new SparkContext(conf.setAppName("TriangleCount GraphX (" + fname + ")").setMaster("spark://172.1.60.11:7077"))
        val graph = GraphLoader.edgeListFile(sc, fname,
          canonicalOrientation = true,
          numEdgePartitions = numEPart,
          edgeStorageLevel = edgeStorageLevel,
          vertexStorageLevel = vertexStorageLevel)
          // TriangleCount requires the graph to be partitioned
          .partitionBy(partitionStrategy.getOrElse(RandomVertexCut)).cache()
        val triangles = TriangleCount.run(graph)
	/* val tri = triangles.vertices.map{
          case (vid, data) => data.toLong
        }.distinct
	tri.saveAsTextFile(outfname) */
         println("Triangles: " + triangles.vertices.map {
          case (vid, data) => data.toLong
        }.reduce(_ + _) / 3)
        sc.stop()

      case _ =>
        println("Invalid task type.")
    }
  }
}
// scalastyle:on println
