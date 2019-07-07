import org.apache.beam.sdk.transforms.DoFn

class LetterFilterer(val letterToFilter: String) : DoFn<String, String>() {

    @ProcessElement
    fun filterLetterFromString(streamContext: ProcessContext) {
        println("Going through $letterToFilter")
        val element = streamContext.element()
        if (element.contains(letterToFilter, true)) {
            streamContext.output(element)
        } else {
            println("Denied")
            streamContext.output(toBadWordsTag, element)
        }
    }
}