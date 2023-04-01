import apache_beam as beam
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
import apache_beam.runners.interactive.interactive_beam as ib
from apache_beam.options import pipeline_options

options = pipeline_options.PipelineOptions()
p = beam.Pipeline(InteractiveRunner(), options=options)
users = (
    p
    | "read" >> beam.io.ReadFromBigQuery(
        query="select * from `alk-big-data-processing.w1_live.big_lake_users`",
        use_standard_sql=True, project="alk-big-data-processing",
        gcs_location="gs://alk-big-data-processing-temp/beam-temp"
    )
)
ib.show(users, visualize_data=True)
