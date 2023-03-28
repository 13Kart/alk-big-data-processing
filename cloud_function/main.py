from datetime import datetime

import functions_framework
from googleapiclient.discovery import build


@functions_framework.cloud_event
def trigger_dataflow(cloud_event):
    dataflow = build("dataflow", "v1b3")
    project_id = "alk-big-data-processing"
    data = cloud_event.data
    file = data.get("name")
    filename: str = file.split(".")[0]
    request = dataflow.projects().locations().flexTemplates().launch(
        projectId=project_id,
        location="europe-central2",
        body={
            "launch_parameter": {
                "container_spec_gcs_path": "gs://alk-big-data-processing-w3/dataflow-flex-templates/wordcount_bq.json",
                "job_name": f"wordcount_bq_{datetime.now().strftime('%Y%m%d-%H-%M')}",
                "parameters": {
                    "input": f"gs://{data.get('bucket')}/{file}",
                    "output": f"{project_id}.w3.{filename}",
                    "temp_location": "gs://alk-big-data-processing-temp/beam-temp"
                }
            }
        }
    )
    response = request.execute()
    print(response)
