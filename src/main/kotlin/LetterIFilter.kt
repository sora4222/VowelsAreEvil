import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.TupleTag

class LetterFilterer(val letterToFilter: String) : DoFn<String, String>() {
    val mainTopic:TupleTag<String>

    init {
        mainTopic = object : TupleTag<String>(){}
    }

    @ProcessElement
    fun filterLetterFromString(streamContext: ProcessContext) {
        println("Going through $letterToFilter")
        val element = streamContext.element()
        if (!element.contains(letterToFilter, true)) {
            println("Passed through")
            streamContext.output(element)
        } else {
            println("Denied")
            streamContext.output(toBadWordsTopic, element)
        }
    }
}