import org.apache.spark.{SparkContext, SparkConf}
import scala.io.Source
import scala.collection.immutable
import scala.util.Try

object Task1 {
  def main(args: Array[String]) {
   
    val conf = new SparkConf().setAppName("T1")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0)).cache();
    val lines = textFile.map(line => line.split(","))
    val my_map_returned = lines.map(line=> (my_map(line))).saveAsTextFile(args(1));
  }
  

    
    def removeFirst[A](xs: Array[String]) = xs.drop(1)
    
   
    def toInt(s: String): Int = {
        if(s != "")
            return Integer.parseInt(s.trim)
        else 
            return 0;
/*         try {
            return Integer.parseInt(s.trim)

        } catch {
            case e: Exception => return 0
        } */
    }
    
    def my_map(s: Array[String]): String = {
        //println("input string issss: "+ s(0))
        //val name_of_movie = s(0);
        var  list_of_max_indices= s(0)
        val rates =  0 +: s.drop(1).map(x=>toInt(x))
        
        if(!rates.isEmpty){
            val max = rates.reduceLeft(_ max _)
            list_of_max_indices = list_of_max_indices + ","  + rates.zipWithIndex.filter(_._1 == max).map(_._2).mkString(",")
        }
        return  list_of_max_indices;
    }
}
