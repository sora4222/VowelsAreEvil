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

val ipAddress = "3.106.52.86:9092"
val inTopic = "inputTopic"
val outTopic = "outputTopic"
val badWordsTopic = "deadletterTopic"
var nextMainTopic: TupleTag<String>? = null
/**
 * @author: Jesse Ross
 */
fun main() {
    val pipeline = Pipeline.create()

    val inputValuesOnly = pipeline.apply(
        KafkaIO.read<String, String>()
            .withKeyDeserializer(StringDeserializer::class.java)
            .withValueDeserializer(StringDeserializer::class.java)
            .withBootstrapServers(ipAddress)
            .withTopic(inTopic)
    )
        .apply(MapElements.into(TypeDescriptors.strings())
            .via(SerializableFunction { s: KafkaRecord<String, String> -> s.kv.value })
        )

    val filterLetterI = LetterFilterer("I")
    val filterLetterA = LetterFilterer("A")
    val filterLetterE = LetterFilterer("E")
    val filterLetterO = LetterFilterer("O")
    val filterLetterU = LetterFilterer("U")

    // Filters
    val filteredA = filterAndSendToDeadLetterTopicBadWords(filterLetterA, inputValuesOnly)
    val filteredE = filterAndSendToDeadLetterTopicBadWords(filterLetterE, filteredA.get(nextMainTopic))
    val filteredI = filterAndSendToDeadLetterTopicBadWords(filterLetterI, filteredE.get(nextMainTopic))
    val filteredO = filterAndSendToDeadLetterTopicBadWords(filterLetterO, filteredI.get(nextMainTopic))
    val filteredU = filterAndSendToDeadLetterTopicBadWords(filterLetterU, filteredO.get(nextMainTopic))

    outputToAppropriateKafkaTopic(filteredU.get(nextMainTopic), outTopic)
    outputToAppropriateKafkaTopic(filteredU.get(toBadWordsTopic), badWordsTopic)

    pipeline.run().waitUntilFinish()
}

fun filterAndSendToDeadLetterTopicBadWords(filter: LetterFilterer, inputCollection: PCollection<String>):
        PCollectionTuple {
    nextMainTopic = filter.mainTopic
    val mainAndBadWordsTopic: PCollectionTuple =
        inputCollection.apply(ParDo.of(filter).withOutputTags(filter.mainTopic, TupleTagList.of(toBadWordsTopic)))
    outputToAppropriateKafkaTopic(mainAndBadWordsTopic.get(filter))
    return mainAndBadWordsTopic
}

fun outputToAppropriateKafkaTopic(filteredU: PCollection<String>, outTopic: String):PDone {
    return filteredU.apply(KafkaIO.write<Unit,String>()
        .withBootstrapServers(ipAddress)
        .withTopic(outTopic)
        .withValueSerializer(StringSerializer::class.java)
        .values())
}
