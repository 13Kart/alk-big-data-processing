from datetime import datetime
import os

import functions_framework
from googleapiclient.discovery import build


@functions_framework.cloud_event
def trigger_dataflow(cloud_event):
    dataflow = build("dataflow", "v1b3")
    project_id = os.environ.get("DATAFLOW_GCP_PROJECT_ID")
    location = os.environ.get("DATAFLOW_JOB_LOCATION")
    template_location = os.environ.get("TEMPLATE_LOCATION")
    temp_location = os.environ.get("TEMP_LOCATION")
    dataset = os.environ.get("DATASET")
    data = cloud_event.data
    file = data.get("name")
    filename: str = file.split(".")[0]
    request = dataflow.projects().locations().flexTemplates().launch(
        projectId=project_id,
        location=location,
        body={
            "launch_parameter": {
                "container_spec_gcs_path": template_location,
                "job_name": f"wordcount-bq-{datetime.now().strftime('%Y%m%d-%H-%M')}",
                "parameters": {
                    "input": f"gs://{data.get('bucket')}/{file}",
                    "output": f"{project_id}.{dataset}.{filename}",
                    "temp_location": temp_location
                }
            }
        }
    )
    response = request.execute()
    print(response)
