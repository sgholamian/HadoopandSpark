import org.apache.spark.{SparkContext, SparkConf}
import scala.io.Source
import scala.collection.immutable
import scala.util.Try

object Task2 {
    def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("T2")
    val sc = new SparkContext(conf)

        val textFile = sc.textFile(args(0)).cache();
        val lines = textFile.map(line => line.split(",",-1))
        val my_map_returned = lines.map(line=> (1, my_map(line))).reduceByKey(_+_).map(x=>x._2).coalesce(1,true).saveAsTextFile(args(1))

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
    
    def my_map(s: Array[String]): Int = {
        //println("input string issss: "+ s(0))
        val name_of_movie = s(0);
        val rates = s.drop(1).map(x=>toInt(x))
        var i=0
        var sum =0;
        while (i < rates.length) {
            if(rates(i)>0)
                sum += 1
            i += 1
        }
        val max = rates.reduceLeft(_ max _)
        val  list_of_max_indices = s(0) + ","  + rates.zipWithIndex.filter(_._1 == max).map(_._2).mkString(",")
        return  sum;

    }
}
