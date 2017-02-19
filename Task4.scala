import org.apache.spark.{SparkContext, SparkConf}
import scala.io.Source
import scala.collection.immutable
import scala.util.Try
import scala.math.BigDecimal

object Task4 {
    def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("Spark T4")
    val sc = new SparkContext(conf)

        val textFile = sc.textFile(args(0)).cache();
        val lines = textFile.map(line => line.split(",",-1))
        val cartition =  lines.cartesian(lines).filter{ case (a,b) => a(0) < b(0)}.map(x=>my_map(x._1,x._2)).saveAsTextFile(args(1))

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
    
    def my_map(a: Array[String], b:Array[String]): String = {
        val name_of_movie = a(0)+","+b(0)+",";
        val a_vals = a.drop(1).map(x=>toInt(x))
        val b_vals = b.drop(1).map(x=>toInt(x))
        var i=0
        var a_sum =0.0;
        var b_sum = 0.0
        var mul = 0.0
        while (i < a_vals.length) {
            mul = mul + a_vals(i)*b_vals(i);
            a_sum= a_sum + Math.pow(a_vals(i).toDouble,2.0)
            b_sum= b_sum + Math.pow(b_vals(i).toDouble,2.0)
            i += 1
        }
        
        val divider = Math.sqrt(a_sum)* Math.sqrt(b_sum)
        var co_sim : String = "0.00";
        if(divider>0.0){
            
            co_sim = "%1.2f".format((mul / divider))
           // co_sim = (BigDecimal((mul / divider)).setScale(2, BigDecimal.RoundingMode.HALF_UP).toString)
        }
/*         else
        {
            co_sim  = "0.00"
        }  */
        co_sim = name_of_movie + co_sim
        return  co_sim;
    }
}
