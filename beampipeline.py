import argparse
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions


class ToLower(beam.DoFn):
    def process(self, element, *args, **kwargs):
        print(element.lower())
        return [{'data': element.lower()}]


def select(lines):
    return lambda x: x['data']


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", dest='input', default='/Users/akute/PycharmProjects/test/test_input.txt',
                        help="Input file path")
    parser.add_argument("--output", dest='output', default='/Users/akute/PycharmProjects/test/test_output.txt',
                        help="Output file path")
    known_args, pipeline_args = parser.parse_known_args(argv)
    # print(arg1.input)
    # print(arg1[1][0])
    # print(arg2)
    # print (type(arg2[0]))
    options = PipelineOptions(pipeline_args)
    # pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    p = beam.Pipeline(options=options)

    line1 = (p | 'read' >> ReadFromText(known_args.input))

    line2 = (line1 | 'Lower' >> beam.ParDo(ToLower()))

   # lines = ("data" >> beam.ParDo(select()))
    line2 | 'write' >> WriteToText(known_args.output)
    return p.run()


if __name__ == "__main__":
    print("job started")
    run()
