package news

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{Dataset, Row, SQLContext, SparkSession}

import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.DataTypes

object Start {
  private val MEDIA = "media"
  private val AUTHOR = "author"
  private val TOPIC = "topic"
  private val WORDS = "words"


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("news_scala")
    val sc = new JavaSparkContext(conf)
    val session = SparkSession.builder.getOrCreate
    val dataString1 = session.read.json("data/news/news.json")
    dataString1.show()


    val politic_words = List("human rights", "Democrats")


    val g: String => String = _.filter(_.equals(politic_words))
    val s = udf(g)
    dataString1.withColumn("pol",s(col(WORDS))).show()
  }
}







