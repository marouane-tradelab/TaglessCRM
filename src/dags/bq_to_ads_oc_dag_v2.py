# coding=utf-8
# Copyright 2022 Google LLC.
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

"""Airflow DAG for uploading Google Ads offline click conversion via Google Ads API."""

import os
from typing import Optional

from datetime import timedelta


from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.models import BaseOperator

import base_dag
from pipeline_plugins.operators import data_connector_operator_v2
from pipeline_plugins.utils import hook_factory

# Airflow configuration variables.
_AIRFLOW_ENV = 'AIRFLOW_HOME'

# Airflow DAG configurations.
_DAG_NAME = 'tcrm_bq_to_ads_oc_v2'

# BigQuery connection ID. Refer to
# https://cloud.google.com/composer/docs/how-to/managing/connections
# for more details on Managing Airflow connections.
_BQ_CONN_ID = 'bigquery_default'

_DEFAULT_API_VERSION = '13'


class BigQueryToAdsOCDagV2(base_dag.BaseDag):
  """BigQuery to Google Ads Offline Conversion DAG."""

  def create_task(
      self,
      main_dag: Optional[DAG] = None,
      is_retry: bool = False) -> data_connector_operator_v2.BigQueryToAdsOCOperator:
    """Creates and initializes the main DAG.

    Args:
      main_dag: The dag that the task attaches to.
      is_retry: Whether or not the operator should includ a retry task.

    Returns:
      DataConnectorOperator.
    """
    return data_connector_operator_v2.BigQueryToAdsOCOperator(
        dag_name=_DAG_NAME,
        task_id=self.get_task_id('bq_to_ads_oc', is_retry),
        is_retry=is_retry,
        return_report=self.dag_enable_run_report,
        enable_monitoring=self.dag_enable_monitoring,
        monitoring_dataset=self.monitoring_dataset,
        monitoring_table=self.monitoring_table,
        monitoring_bq_conn_id=self.monitoring_bq_conn_id,
        bq_conn_id=_BQ_CONN_ID,
        bq_dataset_id=self.get_variable_value(_DAG_NAME, 'bq_dataset_id'),
        bq_table_id=self.get_variable_value(_DAG_NAME, 'bq_table_id'),
        api_version=self.get_variable_value(
            _DAG_NAME, 'api_version', fallback_value=_DEFAULT_API_VERSION),
        google_ads_yaml_credentials=self.get_variable_value(
            _DAG_NAME,
            'google_ads_yaml_credentials'),
        dag=main_dag)  # pytype: disable=wrong-arg-types


if os.getenv(_AIRFLOW_ENV):
  with DAG(
      _DAG_NAME,
      default_args={
          'owner': 'airflow',
          'depends_on_past': False,
          'email_on_failure': False,
          'email_on_retry': False,
          'retries': 1,
          'retry_delay': timedelta(minutes=5),
          'start_date': days_ago(1),
      },
      catchup=False,
      schedule_interval=timedelta(days=1),
  ) as dag:

    start = DummyOperator(task_id='start')
    bq_to_ads_task = BigQueryToAdsOCDagV2(_DAG_NAME).create_task(main_dag=dag)
    start >> bq_to_ads_task
