import _root_.kafka.serializer.DefaultDecoder
import _root_.kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming._
import org.apache.log4j.Logger

object SimpleStreaming { 

    val localLogger = Logger.getLogger("SimpleStreaming")

    def main(args: Array[String]){
        val sc = new SparkConf().setAppName("SimpleStreamingApp")
 
        // creating the StreamingContext with 5 seconds interval
        val ssc = new StreamingContext(sc, Seconds(5))

        val kafkaTopicRaw = "test"
        val kafkaBroker = "localhost:9092"
        val topics: Set[String] = kafkaTopicRaw.split(",").map(_.trim).toSet
 
        val kafkaParams = Map[String, String](
            "metadata.broker.list" -> kafkaBroker
        )

        val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
            ssc, 
            kafkaParams, 
            topics
        )
 
        val words = stream.flatMap{case(x, y) => y.split(" ")}
 
        words.print()
 
        ssc.start()
    }
}