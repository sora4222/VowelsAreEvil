import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.kafka.KafkaIO
import org.apache.beam.sdk.io.kafka.KafkaRecord
import org.apache.beam.sdk.transforms.MapElements
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.values.*
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer

val toBadWordsTag: TupleTag<String> = object : TupleTag<String>() {}
val mainTag: TupleTag<String> = object : TupleTag<String>() {}
val ipAddress = "3.104.121.119"
val bootstrapServerAndPort = "$ipAddress:9092"
val inTopic = "inputTopic"
val outTopic = "outputTopic"
val badWordsTopic = "deadletterTopic"

/**
 * @author: Jesse Ross
 */
fun main() {
    val pipeline = Pipeline.create()

    val inputValuesOnly = pipeline.apply(
        KafkaIO.read<String, String>()
            .withKeyDeserializer(StringDeserializer::class.java)
            .withValueDeserializer(StringDeserializer::class.java)
            .withBootstrapServers(bootstrapServerAndPort)
            .withTopic(inTopic)
    )
        .apply(
            MapElements.into(TypeDescriptors.strings())
                .via(SerializableFunction { s: KafkaRecord<String, String> -> s.kv.value })
        )

    val filterLetterI = LetterFilterer("I")
    val filterLetterA = LetterFilterer("A")
    val filterLetterE = LetterFilterer("E")
    val filterLetterO = LetterFilterer("O")
    val filterLetterU = LetterFilterer("U")

    val inputAsCollection: PCollectionTuple = PCollectionTuple.of(mainTag, inputValuesOnly)

    // Filters
    val filteredTupleA = filterBadWordsToKafkaTopic(filterLetterA, inputAsCollection)
    val filteredTupleAE = filterBadWordsToKafkaTopic(filterLetterE, filteredTupleA)
    val filteredTupleAEI = filterBadWordsToKafkaTopic(filterLetterI, filteredTupleAE)
    val filteredTupleAEIO = filterBadWordsToKafkaTopic(filterLetterO, filteredTupleAEI)
    val filteredTupleAEIOU = filterBadWordsToKafkaTopic(filterLetterU, filteredTupleAEIO)

    outputAll(filteredTupleAEIOU)

    pipeline.run().waitUntilFinish()
}

fun outputAll(resultantCollection: PCollectionTuple) {
    outputPCollectionToKafkaTopics(resultantCollection[mainTag], outTopic)
    outputPCollectionToKafkaTopics(resultantCollection[toBadWordsTag], badWordsTopic)
}

/**
 * This outputs all the filtered messages to the dead letter topic on Kafka, and returns the main PCollection
 * with all the non filtered messages
 */
fun filterBadWordsToKafkaTopic(filter: LetterFilterer, inputCollection: PCollectionTuple):
        PCollectionTuple {
    return inputCollection
        .get(mainTag)
        .apply(ParDo.of(filter).withOutputTags(mainTag, TupleTagList.of(toBadWordsTag)))
}

fun outputPCollectionToKafkaTopics(filtered: PCollection<String>, outTopicName: String): PDone {
    return filtered.apply(
        KafkaIO.write<Unit, String>()
            .withBootstrapServers(bootstrapServerAndPort)
            .withTopic(outTopicName)
            .withValueSerializer(StringSerializer::class.java)
            .values()
    )
}
