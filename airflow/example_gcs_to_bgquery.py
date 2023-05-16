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
"""
Example DAG using GCSToBigQueryOperator.
"""
from __future__ import annotations

from datetime import datetime

from airflow import models
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


DAG_ID = "gcs_to_bigquery_operator"

DATASET_NAME = f"dataset_{DAG_ID}"
TABLE_NAME = "test"

with models.DAG(
    dag_id=DAG_ID,
    schedule_interval="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "gcs"],
) as dag:
    create_test_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_airflow_test_dataset", dataset_id=DATASET_NAME, project_id="{{ var.value.gcp_project }}"
    )

    load_csv = GCSToBigQueryOperator(
        task_id="gcs_to_bigquery_example",
        bucket="cloud-samples-data",
        source_objects=["bigquery/us-states/us-states.csv"],
        destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
        schema_fields=[
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "post_abbr", "type": "STRING", "mode": "NULLABLE"},
        ],
        write_disposition="WRITE_TRUNCATE",
    )

    (
        create_test_dataset
        >> load_csv
    )
