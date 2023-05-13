#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

from datetime import datetime

from airflow import models
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.providers.google.cloud.operators.gcs import GCSObjectExistenceSensor

DAG_ID = "dataflow_template"

BUCKET_NAME = f"alk-big-data-processing-w2"

FILE_NAME = "pan-tadeusz.txt"
GCS_TMP = f"gs://{BUCKET_NAME}/temp/"
GCS_STAGING = f"gs://{BUCKET_NAME}/staging/"
GCS_OUTPUT = f"gs://{BUCKET_NAME}/output"
LOCATION = "europe-west3"

default_args = {
    "dataflow_default_options": {
        "tempLocation": GCS_TMP,
        "stagingLocation": GCS_STAGING,
    }
}

with models.DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "dataflow"],
) as dag:
    gcs_object_exists = GCSObjectExistenceSensor(
        bucket=BUCKET_NAME,
        object=FILE_NAME,
        task_id="gcs_object_exists_task",
    )

    start_template_job = DataflowTemplatedJobStartOperator(
        task_id="start_template_job",
        project_id="{{ var.value.gcp_project }}",
        template="gs://dataflow-templates/latest/Word_Count",
        parameters={"inputFile": f"gs://{BUCKET_NAME}/{FILE_NAME}", "output": GCS_OUTPUT},
        location=LOCATION,
    )

    gcs_object_exists >> start_template_job
