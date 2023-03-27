import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        counts = (
            p
            | "Read lines" >> ReadFromText(known_args.input)
            | "Split to words" >> beam.ParDo(WordExtractingDoFn())
            | "Pair with 1" >> beam.Map(lambda word: (word, 1))
            | "Group and sum" >> beam.CombinePerKey(sum)
            | "Format" >> beam.MapTuple(lambda word, count: {"word": word, "count": count})
        )
        table_schema = "word:STRING, count:INT64"
        counts | "Write to BQ" >> WriteToBigQuery(
            table=known_args.output,
            schema=table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )


class WordExtractingDoFn(beam.DoFn):
    def process(self, element):
        return re.findall(r"[\w\"]+", element, re.UNICODE)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
