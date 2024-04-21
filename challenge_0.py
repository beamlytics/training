import apache_beam as beam

class Output(beam.PTransform):
    class _OutputFn(beam.DoFn):
        def __init__(self, prefix=" "):
            super().__init__()
            self.prefix = prefix  

        def process(self, element):
            print(element)


    def __init__(self, label=None,prefix=" "):
        super().__init__(label)
        self.prefix = prefix

    def expand(self,input):
        input | beam.ParDo(self._OutputFn(self.prefix))



class ExtractTaxiRideCostFn(beam.DoFn):

    def process(self,element):
        line = element.split(",")
        return tryParseTaxiRideCost(line,16)
    
def tryParseTaxiRideCost(line, index):
    if (len(line) > index):
        try:
            yield float(line[index])
        except:
            yield float(0)
    else:
        yield float(0)


with beam.Pipeline() as p:

    input = (p | "Log Lines" >> beam.io.ReadFromText("c0.csv")
      | beam.ParDo(ExtractTaxiRideCostFn()))

    (input | "Filter above cost" >> beam.Filter(lambda cost: cost >=15.0)
     |"Sum above cost">> beam.CombineGlobally(sum)
     |"WithKeys above">> beam.WithKeys(lambda cost: "above")
     |"Log above cost">> Output())
    
    (input | "Filter below cost">> beam.Filter(lambda cost: cost <=15.0)
     |"Sum below cost">> beam.CombineGlobally(sum)
     |"WithKeys below">> beam.WithKeys(lambda cost:"below" )
     |"Log below cost">>Output())