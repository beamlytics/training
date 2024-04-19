import apache_beam as beam

# Output PCollection
class Output(beam.PTransform):
    class _OutputFn(beam.DoFn):
        def __init__(self, prefix=''):
            super().__init__()
            self.prefix = prefix

        def process(self, element):
            print(self.prefix+str(element))

    def __init__(self, label=None,prefix=''):
        super().__init__(label)
        self.prefix = prefix

    def expand(self, input):
        input | beam.ParDo(self._OutputFn(self.prefix))

def partition_fn(word, num_partitions):
    return 0

with beam.Pipeline() as p:
  parts = p | 'Log lines' >> beam.io.ReadFromText("E:\Beamlytics\kinglear.txt") \
            | beam.combiners.Sample.FixedSizeGlobally(100) \
            | beam.FlatMap(lambda line: line) \
            | beam.FlatMap(lambda sentence: sentence.split()) \
            | beam.Partition(partition_fn, 3)

  allLetterUpperCase = parts[0] | 'All upper' >> beam.combiners.Count.PerElement() | beam.Map(lambda key: (key[0].lower(),key[1]))
  firstLetterUpperCase = parts[1] | 'First upper' >> beam.combiners.Count.PerElement() | beam.Map(lambda key: (key[0].lower(),key[1]))
  allLetterLowerCase = parts[2] | 'Lower' >> beam.combiners.Count.PerElement()

  flattenPCollection = (allLetterUpperCase, firstLetterUpperCase, allLetterLowerCase) \
            | beam.Flatten() \
            | beam.GroupByKey() \
            | Output()