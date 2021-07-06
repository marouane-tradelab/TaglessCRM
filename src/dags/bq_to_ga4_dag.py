# python3
# coding=utf-8
# Copyright 2020 Google LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Airflow DAG for TCRM workflow.

This DAG will transfer data from BigQuery to Google Analytics 4 via Measurement
Protocol API.

This DAG relies on three Airflow variables:
* `payload_type`:    Payload type. Ex: firebase or gtag.
* `measurement_id`:  Measurement ID for gtag. Ex: `G-02XXXXXXXX`.
* `firebase_app_id`: Firebase App id for firebase. Ex: `1:12345:android:xxx`.
* `api_secret`:      API secret created for accessing GA4 MP API.
* `bq_dataset_id`:   BigQuery dataset name. Ex: `my_dataset`.
* `bq_table_id`:     BigQuery table name which holds the data. Ex: `my_table`

Refer to https://airflow.apache.org/concepts.html#variables for more on Airflow
Variables.
"""

import os
from typing import Optional

from airflow.models import dag
from airflow.models import variable

from dags import base_dag
from plugins.pipeline_plugins.operators import data_connector_operator
from plugins.pipeline_plugins.utils import hook_factory

# Airflow configuration variables.
_AIRFLOW_ENV = 'AIRFLOW_HOME'

# Airflow DAG configurations.
_DAG_NAME = 'tcrm_bq_to_ga4'

# BigQuery connection ID. Refer to
# https://cloud.google.com/composer/docs/how-to/managing/connections
# for more details on Managing Airflow connections.
_BQ_CONN_ID = 'bigquery_default'


class BigQueryToGA4Dag(base_dag.BaseDag):
  """BigQuery to Google Analytics 4 DAG."""

  def create_task(self,
                  main_dag: Optional[dag.DAG] = None,
                  is_retry: bool = False
                 ) -> data_connector_operator.DataConnectorOperator:
    """Creates and initializes the main DAG.

    Args:
      main_dag: The dag that the task attaches to.
      is_retry: Whether or not the operator should includ a retry task.

    Returns:
      DataConnectorOperator.
    """
    return data_connector_operator.DataConnectorOperator(
        dag_name=self.dag_name,
        task_id=self.get_task_id('bq_to_ga4', is_retry),
        input_hook=hook_factory.InputHookType.BIG_QUERY,
        output_hook=hook_factory.OutputHookType.GOOGLE_ANALYTICS_4,
        is_retry=is_retry,
        return_report=self.dag_enable_run_report,
        enable_monitoring=self.dag_enable_monitoring,
        monitoring_dataset=self.monitoring_dataset,
        monitoring_table=self.monitoring_table,
        monitoring_bq_conn_id=self.monitoring_bq_conn_id,
        bq_conn_id=_BQ_CONN_ID,
        bq_dataset_id=variable.Variable.get('bq_dataset_id', ''),
        bq_table_id=variable.Variable.get('bq_table_id', ''),
        api_secret=variable.Variable.get('api_secret', ''),
        payload_type=variable.Variable.get('payload_type', ''),
        measurement_id=variable.Variable.get('measurement_id', ''),
        firebase_app_id=variable.Variable.get('firebase_app_id', ''),
        dag=main_dag)


if os.getenv(_AIRFLOW_ENV):
  dag = BigQueryToGA4Dag(_DAG_NAME).create_dag()
