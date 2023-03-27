import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
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
            | "Format" >> beam.MapTuple(lambda word, count: f"{word}: {count}")
        )
        counts | "Write" >> WriteToText(known_args.output)


class WordExtractingDoFn(beam.DoFn):
    def process(self, element):
        return re.findall(r"[\w\"]+", element, re.UNICODE)


def split_to_words(line):
    for word in re.findall(r"[\w\"]+", line, re.UNICODE):
        yield word


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
