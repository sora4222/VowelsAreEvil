import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.kafka.KafkaIO
import org.apache.beam.sdk.io.kafka.KafkaRecord
import org.apache.beam.sdk.transforms.MapElements
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.values.*
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer

val toBadWordsTopic: TupleTag<String> = object : TupleTag<String>() {}

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

    // Filters
    val filteredA = filterBadWordsToKafkaTopic(filterLetterA, inputValuesOnly)
    val filteredAE = filterBadWordsToKafkaTopic(filterLetterE, filteredA)
    val filteredAEI = filterBadWordsToKafkaTopic(filterLetterI, filteredAE)
    val filteredAEIO = filterBadWordsToKafkaTopic(filterLetterO, filteredAEI)
    val filteredAEIOU = filterBadWordsToKafkaTopic(filterLetterU, filteredAEIO)

    outputPCollectionToKafkaTopic(filteredAEIOU, outTopic)

    pipeline.run().waitUntilFinish()
}

/**
 * This outputs all the filtered messages to the dead letter topic on Kafka, and returns the main PCollection
 * with all the non filtered messages
 */
fun filterBadWordsToKafkaTopic(filter: LetterFilterer, inputCollection: PCollection<String>):
        PCollection<String> {
    val mainAndBadWordsTopic: PCollectionTuple =
        inputCollection.apply(ParDo.of(filter).withOutputTags(filter.mainTopic, TupleTagList.of(toBadWordsTopic)))
    outputPCollectionToKafkaTopic(mainAndBadWordsTopic.get(toBadWordsTopic), badWordsTopic)
    return mainAndBadWordsTopic.get(filter.mainTopic)
}

fun outputPCollectionToKafkaTopic(resultantOutput: PCollection<String>, outTopicName: String): PDone {
    return resultantOutput.apply(
        KafkaIO.write<Unit, String>()
            .withBootstrapServers(bootstrapServerAndPort)
            .withTopic(outTopicName)
            .withValueSerializer(StringSerializer::class.java)
            .values()
    )
}
