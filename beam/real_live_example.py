import argparse
import csv
import gzip
import logging
from io import BytesIO
from typing import Dict

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io import WriteToBigQuery, fileio
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.options.pipeline_options import PipelineOptions


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--date", required=True)
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        encrypted_file = (
            p
            | "Look for file" >> fileio.MatchFiles(known_args.input)
            | "Get file" >> fileio.ReadMatches(compression=CompressionTypes.UNCOMPRESSED)
        )

        decrypted_file = (
            encrypted_file
            | "Decrypt" >> beam.Map(decrypt)
        )

        decompressed_file = (
            decrypted_file
            | "Decompress" >> beam.Map(decompress)
        )

        rows, validation_header, schema_header, trailer = (
            decompressed_file
            | "Split file into rows" >> beam.FlatMap(split_rows).with_outputs(
                "validation_header", "schema_header", "trailer", main="rows"
            )
        )

        validated_header = (
            validation_header
            | "Validate header" >> beam.Map(validate_header, known_args.date)
        )
        rows_count = (
            rows
            | "Count rows" >> beam.combiners.Count.Globally()
        )

        validated_trailer = (
            trailer
            | "Validate trailer" >> beam.Map(validate_trailer, pvalue.AsSingleton(rows_count))
        )

        if validated_header and validated_trailer:
            fieldnames = (
                schema_header
                | "Split to field names" >> beam.Map(lambda line: line.split(","))
            )
            csv_rows = (
                rows
                | "Parse csv rows" >> beam.MapTuple(parse_csv_rows, pvalue.AsSingleton(fieldnames))
            )
            (
                csv_rows
                | "Write to BQ" >> WriteToBigQuery(
                    table=known_args.output,
                    schema="SCHEMA_AUTODETECT",
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                )
            )


def decrypt(file: fileio.ReadableFile) -> bytes:
    # some decryption logic
    return file.read()


def decompress(content: bytes) -> BytesIO:
    decompressed = gzip.decompress(content)
    return BytesIO(decompressed)


def split_rows(content: BytesIO):
    yield pvalue.TaggedOutput("validation_header", content.readline().decode("utf-8").strip())
    yield pvalue.TaggedOutput("schema_header", content.readline().decode("utf-8").strip())
    line = content.readline().decode("utf-8").strip()
    row_number = 1
    while line:
        next_line = content.readline().decode("utf-8").strip()
        if next_line:
            yield line, row_number
        else:
            yield pvalue.TaggedOutput("trailer", int(line))
        line = next_line
        row_number += 1


def validate_header(header: str, date: str):
    if header == date:
        return True
    else:
        raise ValueError("Validation for header failed")


def validate_trailer(trailer_count: int, rows_count: int):
    if rows_count == trailer_count:
        return True
    else:
        raise ValueError("Validation for trailer failed")


def parse_csv_rows(line: str, row_number: int, fieldnames=None) -> Dict:
    [line_dict] = csv.DictReader([line], fieldnames=fieldnames)
    line_dict["row_number"] = row_number
    return line_dict


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
