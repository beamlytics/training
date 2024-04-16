import apache_beam as beam


# Output PCollection
class Output(beam.PTransform):
    class _OutputFn(beam.DoFn):

        def process(self, element):
            print(element)

    def expand(self, input):
        input | beam.ParDo(self._OutputFn())

with beam.Pipeline() as p:
  (p | beam.Create(['Hello Ritesh'])
   | Output())

