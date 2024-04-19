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


with beam.Pipeline() as p:
  parts = p | 'Log words' >> beam.io.ReadFromText("E:/Beamlytics/gaming_data.csv") \
            | beam.combiners.Sample.FixedSizeGlobally(100) \
            | beam.FlatMap(lambda line: line) \
            | beam.Map(lambda line: (line.split(',')[1], int(line.split(',')[2]))) \
            | beam.CombinePerKey(sum) \
            | Output()             