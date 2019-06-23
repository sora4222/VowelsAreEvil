import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.TupleTag

class LetterFilterer(val letterToFilter: String) : DoFn<String, String>() {
    val mainTopic:TupleTag<String> = object : TupleTag<String>(){}

    @ProcessElement
    fun filterLetterFromString(streamContext: ProcessContext) {
        val element = streamContext.element()
        if (element.contains(letterToFilter, true)) {
            streamContext.output(element)
        } else {
            streamContext.output(toBadWordsTopic, element)
        }
    }
}