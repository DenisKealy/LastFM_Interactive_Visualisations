/* ################# 
### Spark Shell  ###
##################*/

// Read in a small sample (44 songs - 3800 edges )
val df = spark.read.json("/Users/admin/workspace/Datasets/sample_lastfm/A/*.json")

// Attempt to read a bigger sample to perform meaningful community detection
val df = spark.read.json("/Users/admin/workspace/Datasets/sample_lastfm/*/*.json")

// Large sample: ~3.8% of total data
val df = spark.read.json("/Users/admin/workspace/Datasets/lastfm_train/A/*/*/*.json")
// scala> df.count()
// res0: Long = 32989 <= 33,000 songs

// Load dataset from Amazon S3 bucket
val df = spark.read.json("s3://last-fm-data/json/all.json")

/* #################### 
### Pre-processing  ###
#####################*/

// Create edges and vertices dataframes
var edges = df.withColumn("similars", explode($"similars")).select("similars", "track_id")
edges = edges.select('track_id as 'src, 'similars.getItem(0) as 'dst, 'similars.getItem(1) as 'similar)

var vertices = df.select('track_id as 'id,'title, 'artist, 'tags)

// Load edges & vertices from disk
val edges = spark.read.json("/Users/admin/workspace/Datasets/output/edges/*.json")
val vertices = spark.read.json("/Users/admin/workspace/Datasets/output/vertices/*.json")

// Create graph from dataframes
import org.graphframes._
import org.graphframes.GraphFrame

val g = GraphFrame(vertices, edges)

/* ###################### 
### Graph Processing  ###
#######################*/

// This calculates the degrees of each vortex
// which is the number of total, incoming & outgoing links
val degrees = g.degrees
val inDegrees = g.inDegrees
val outDegrees = g.outDegrees

// Run LPA - returns dataframe of vertices plus a label column
val community_labels = g.labelPropagation.maxIter(30).run()

// Run PageRank for a fixed number of iterations.
var p_rank = g.pageRank.resetProbability(0.15).maxIter(10).run()

// Joining and Subgraphing our Results
var output_vertices = community_labels.join(degrees, "id")
output_vertices = output_vertices.join(inDegrees, "id")
output_vertices = output_vertices.join(outDegrees, "id")
output_vertices = output_vertices.join(p_rank, "id")

var top500_p_rank = new_output_vertices.orderBy(desc("p_rank")).limit(500)

/* ####################### 
### Examining Results  ###
########################*/

// import functions such as desc()
import org.apache.spark.sql.functions._
// count the number of members in each community
var communities = community_labels.groupBy("label").count()
// order by the largest communities
var com_sorted = communities.orderBy(desc("count"))

// Viewing our results using Spak SQL
com_sorted.select("artist", "title", "inDegree", "outDegree", "degree").show(50)
top500_p_rank.select("artist", "title", "inDegree", "outDegree", "degree").show(50)


/* ####################### 
### Exporting Results  ###
########################*/

// Function to write dataframes to valid json
// e.g. toValidJson(edges, "/Users/admin/workspace/Datasets/output/small.json")
def toSingleFileValidJson(df:org.apache.spark.sql.DataFrame, path: String) = {
    var jsonDs = df.toJSON
    var linecount = jsonDs.count()
    jsonDs
    .coalesce(1) // make sure it is only one partition and in consequence one output file
    .rdd
    .zipWithIndex()
    .map { case(json, idx) =>
        if(idx == 0) "{\n" + json + "," // first row
        else if(idx == linecount-1) json + "\n}" // last row
        else json + ","
    }
    .saveAsTextFile(path)
}

// Left SQL join will eliminate all stray edges based on how many vertices we choose
var top500_p_rank_edges = top500_p_rank.join(edges, top500_p_rank("id") === edges("dst")).select("src", "dst", "weight", "similar")


// write edges & vertices to disk
top_500_p_rank_edges.write.json("/Users/admin/workspace/Datasets/visualisation/edges")
top500_p_rank.vertices.write.json("/Users/admin/workspace/Datasets/visualisation/vertices")
// write communities to disk ordered by size
com_sorted.write.json("/Users/admin/workspace/Datasets/output/community_by_size/")



//  From https://www.jianshu.com/p/48eaacaa4736
def graphFrametoGexf(g: GraphFrame) : String = {
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
        "<gexf xmlns=\"http://www.gephi.org/gexf\" xmlns:viz=\"http://www.gephi.org/gexf/viz\">\n" +
        "  <graph mode=\"static\">\n" +
        "    <nodes>\n" +
        g.vertices.rdd.map(v => if(v.get(4)=="p")
        {"      <node id=\"" + v.get(0) + "\" label=\"" +
          v.get(1) + "\">\n        <viz:color b=\"51\" g=\"51\" r=\"255\"/>\n      </node>\n"} else
        {"      <node id=\"" + v.get(0) + "\" label=\"" +
          v.get(1) + "\">\n        <viz:color b=\"255\" g=\"51\" r=\"51\"/>\n      </node>\n"
        }).collect.mkString +
        "    </nodes>\n" +
        "    <edges>\n" +
        g.edges.rdd.map(e =>
        "    <edge source=\"" + e.get(0) +
          "\" target=\"" + e.get(1) + "\" label=\"" + e.get(2) +
          "\" />\n").collect.mkString +
        "    </edges>\n" +
        "  </graph>\n" +
        "</gexf>"
    }


// From https://livebook.manning.com/#!/book/spark-graphx-in-action/chapter-4/153
// - edited to work with GraphFrame instead of Graphx

def toGexf(g:GraphFrame) =
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
    "<gexf xmlns=\"http://www.gexf.net/1.2draft\" version=\"1.2\">\n" +
    "  <graph mode=\"static\" defaultedgetype=\"directed\">\n" +
    "    <nodes>\n" +
    g.vertices.rdd.map(v => "      <node id=\"" + v.get(0) + "\" label=\"" +
                        v.get(1) + "\" />\n").collect.mkString +
    "    </nodes>\n" +
    "    <edges>\n" +
    g.edges.rdd.map(e => "      <edge source=\"" + e.get(0) +
                     "\" target=\"" + e.get(1) + "\" weight=\"" + e.get(2) +
                     "\" />\n").collect.mkString +
    "    </edges>\n" +
    "  </graph>\n" +
    "</gexf>"

val pw = new java.io.PrintWriter("test.gexf") 
pw.write(toGexf(g))
pw.close

