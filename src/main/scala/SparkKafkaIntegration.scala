import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object SparkKafkaIntegration {

  def main(args: Array[String]): Unit = {

    val logger = LogUtils.getLogger(this.getClass.getSimpleName)

    val conf = new SparkConf().setAppName("SparkKafkaIntegration").setMaster("local").set("spark.streaming.kafka.maxRatePerPartition", "100")
    val sc = new SparkContext(conf)
    //sc.driver.allowMultipleContexts = true
    val ssc = new StreamingContext(sc, Seconds(10))

    logger.info(s"Spark context started: $ssc")
    val kafkaParams = Map[String,Object](

      "bootstrap.servers" -> "localhost:9092",

      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "kafka_demo_group",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    val topics = Array("test1","test2")

    val stream = KafkaUtils.createDirectStream(ssc,PreferConsistent, Subscribe[String,String](topics,kafkaParams) )

    stream.map(record=>record.value()).flatMap(line=>line.split(" "))
      .map(word=>word.toLowerCase)
      .map(word=>(word,1))
      .reduceByKey(_+_).saveAsTextFiles("C:\\Users\\HP\\IdeaProjects\\SparkKafkaIntegration\\src\\main\\resources\\output")

    ssc.start()
    ssc.awaitTermination()


  }
}