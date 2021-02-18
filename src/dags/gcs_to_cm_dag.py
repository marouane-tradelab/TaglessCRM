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

This DAG will transfer data from GCS to Campaign Manager (DCM).

This DAG relies on the following Airflow variables:
* `cm_service_account`:Service account authorized as Campaign Manager user.
* `cm_profile_id`:     Profile id of the service account user in CM.
* `gcs_bucket_name`:   Google Cloud Storage bucket name. Ex: 'my_bucket'.
* `gcs_bucket_prefix`: Google Cloud Storage folder name where data is stored.
                       Ex: 'my_folder'.
* `gcs_content_type`:  Google Cloud Storage file format. Either 'JSON' or 'CSV'.


Refer to https://airflow.apache.org/concepts.html#variables for more on Airflow
Variables.
"""

import os
from airflow import models

from dags import base_dag
from plugins.pipeline_plugins.operators import data_connector_operator
from plugins.pipeline_plugins.utils import hook_factory

# Airflow configuration variables.
_AIRFLOW_ENV = 'AIRFLOW_HOME'

# Airflow DAG configurations.
_DAG_NAME = 'tcrm_gcs_to_cm'

# GCS configuration.
_GCS_CONTENT_TYPE = 'JSON'


class GCSToCMDag(base_dag.BaseDag):
  """Cloud Storage to Campaign Manager DAG."""

  def create_task(self, main_dag: models.DAG = None, is_retry: bool = False
                 ) -> data_connector_operator.DataConnectorOperator:
    """Creates and initializes the main DAG.

    Args:
      main_dag: The dag that the task attaches to.
      is_retry: Whether or not the operator should includ a retry task.

    Returns:
      DataConnectorOperator.
    """
    return data_connector_operator.DataConnectorOperator(
        dag_name=_DAG_NAME,
        task_id=self.get_task_id('gcs_to_cm', is_retry),
        input_hook=hook_factory.InputHookType.GOOGLE_CLOUD_STORAGE,
        output_hook=(hook_factory.OutputHookType.
                     GOOGLE_CAMPAIGN_MANAGER_OFFLINE_CONVERSIONS),
        is_retry=is_retry,
        return_report=self.dag_enable_run_report,
        enable_monitoring=self.dag_enable_monitoring,
        monitoring_dataset=self.monitoring_dataset,
        monitoring_table=self.monitoring_table,
        monitoring_bq_conn_id=self.monitoring_bq_conn_id,
        gcs_bucket=models.Variable.get('gcs_bucket_name', ''),
        gcs_prefix=models.Variable.get('gcs_bucket_prefix', ''),
        gcs_content_type=models.Variable.get('gcs_content_type',
                                             _GCS_CONTENT_TYPE).upper(),
        cm_service_account=models.Variable.get('cm_service_account', ''),
        cm_profile_id=models.Variable.get('cm_profile_id', ''),
        dag=main_dag)


if os.getenv(_AIRFLOW_ENV):
  dag = GCSToCMDag(_DAG_NAME).create_dag()
