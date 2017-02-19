import org.apache.spark.{SparkContext, SparkConf}
import scala.io.Source
import scala.collection.immutable
import scala.util.Try
import scala.math.BigDecimal

object Task3 {
    def main(args: Array[String]) {
    
        val conf = new SparkConf().setAppName("T3")
        val sc = new SparkContext(conf)

        val textFile = sc.textFile(args(0)).cache();
        val lines = textFile.map(line => line.split(","))
        val line_count=lines.count()
        val my_map_returned = lines.map(line => my_ungroupung_v2(line)).flatMap(x=>x)
            
                       

        type ScoreCollector = (Int, Double)
        type OverallVal = (Int, (Int, Double))

        val createScoreCombiner = (score: Double) => (1, score)

        val scoreCombiner = (collector: ScoreCollector, score: Double) => {
                 val (numberScores, totalScore) = collector
                (numberScores + 1, totalScore + score)
              }

        val scoreMerger = (collector1: ScoreCollector, collector2: ScoreCollector) => {
              val (numScores1, totalScore1) = collector1
              val (numScores2, totalScore2) = collector2
              (numScores1 + numScores2, totalScore1 + totalScore2)
            }
        val scores = my_map_returned.combineByKey(createScoreCombiner, scoreCombiner, scoreMerger)

        val averagingFunctionMovies = (moveAvg: OverallVal) => {
               val (name, (numberScores, totalScore)) = moveAvg
               //(name, (BigDecimal((totalScore / numberScores)).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble))
               (name, "%1.1f".format(totalScore / numberScores))
            }

        val averageScores = scores.map(averagingFunctionMovies).map { case (int, double) => s"$int,$double"}.saveAsTextFile(args(1));
        
                   
        
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
    
 
    
    def my_ungroupung_v2(s: Array[String]): List[(Int , Double)] = {
        var return_this: List[(Int, Double)] =  List ()
        val name_of_movie = s(0);
        val rates = 0.toDouble +: s.drop(1).map(x=>toInt(x).toDouble)
        var i=1
        var sum =0;
        while (i < rates.length) {
            if(  0 <  rates(i) )
                return_this = (i, rates(i)) :: return_this  //.add((i,rates(i)))
            i=i+1
        }
        return  return_this;
    }
    
   
    
}
