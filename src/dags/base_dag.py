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

"""Base Airflow DAG for TCRM workflow."""

import abc
import datetime
import logging
from typing import Any, Optional

from airflow import DAG, utils
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.models.variable import Variable
from airflow.models import variable

from pipeline_plugins.operators import error_report_operator
from pipeline_plugins.operators import monitoring_cleanup_operator
from pipeline_plugins.utils import errors

# Airflow DAG configurations.
_DAG_RETRIES = 0
_DAG_RETRY_DELAY_MINUTES = 3
_DAG_SCHEDULE = '@once'

# Indicates whether the tasks will return a run report or not. The report will
# be returned as the operator's output. Not all operators have reports.
_ENABLE_RETURN_REPORT = False

# Whether or not the DAG should use the monitoring storage for logging.
# Enable monitoring to enable retry and reporting later.
_DAG_ENABLE_MONITORING = True
_DEFAULT_MONITORING_DATASET_ID = 'tcrm_monitoring_dataset'
_DEFAULT_MONITORING_TABLE_ID = 'tcrm_monitoring_table'

# Whether or not the cleanup operator should run automatically after a DAG
# completes.
_DEFAULT_DAG_ENABLE_MONITORING_CLEANUP = False
# Number of days data can live in the monitoring table before being removed.
_DEFAULT_MONITORING_DATA_DAYS_TO_LIVE = 50

# BigQuery connection ID for the monitoring table. Refer to
# https://cloud.google.com/composer/docs/how-to/managing/connections
# for more details on Managing Airflow connections.
# This could be the same or different from the input BQ connection ID.
_MONITORING_BQ_CONN_ID = 'bigquery_default'

# Whether or not the DAG should include a retry task. This is an internal retry
# to send failed events from previous similar runs. It is different from the
# Airflow retry of the whole DAG.
# If True, the input resource will be the monitoring BigQuery table and dataset
# (as described in _MONITORING_DATASET and _MONITORING_TABLE). Previously failed
# events will be resent to the same output resource.
_DAG_IS_RETRY = True

# Whether or not the DAG should include a main run. This option can be used
# should the user want to skip the main run and only run the retry operation.
_DAG_IS_RUN = True


def create_error_report_task(
    error_report_dag: Optional[DAG],
    error: Exception) -> error_report_operator.ErrorReportOperator:
  """Creates an error task and attaches it to the DAG.

  In case there was an error during a task creation in the DAG, that task will
  be replaced with an error task. An error task will make the DAG
  fail with the appropriate error message from the initial task so the user
  can fix the issue that prevented the task creation (e.g. due to missing
  params).

  Args:
    error_report_dag: The DAG that tasks attach to.
    error: The error to display.

  Returns:
    An ErrorReportOperator instance task.
  """
  return error_report_operator.ErrorReportOperator(
      task_id='configuration_error',
      error=error,
      dag=error_report_dag)


class BaseDag(abc.ABC):
  """Base Airflow DAG.

  Attributes:
    dag_name: The name of the dag.
    dag_retries: The retry times for the dag.
    dag_retry_delay: The interval between Airflow DAG retries.
    dag_schedule: The schedule for the dag.
    dag_is_retry: Whether or not the DAG should include a retry task. This is
                  an internal retry to send failed events from previous
                  similar runs. It is different from the Airflow retry of the
                  whole DAG.
    dag_is_run: Whether or not the DAG should include a main run.
    dag_enable_run_report: Indicates whether the tasks will return a run report
                           or not.
    dag_enable_monitoring: Whether or not the DAG should use the monitoring
                           storage for logging. Enable monitoring to enable
                           retry and reporting later.
    dag_enable_monitoring_cleanup: Whether or not the cleanup operator should
                                   run automatically after a DAG completes.
    days_to_live: Number of days data can live in the monitoring table before
                  being removed.
    monitoring_dataset: Dataset id of the monitoring table.
    monitoring_table: Table name of the monitoring table.
    monitoring_bq_conn_id: BigQuery connection ID for the monitoring table.
  """

  def __init__(self, dag_name: str)  -> None:
    """Initializes the base DAG.

    Args:
      dag_name: The name of the DAG.
    """
    self.dag_name = dag_name

    self.dag_retries = int(
        Variable.get(f'{self.dag_name}_retries', _DAG_RETRIES))
    self.dag_retry_delay = int(
        Variable.get(f'{self.dag_name}_retry_delay',
                              _DAG_RETRY_DELAY_MINUTES))
    self.dag_schedule = Variable.get(f'{self.dag_name}_schedule',
                                              _DAG_SCHEDULE)
    self.dag_is_retry = bool(
        int(Variable.get(f'{self.dag_name}_is_retry', _DAG_IS_RETRY)))
    self.dag_is_run = bool(
        int(Variable.get(f'{self.dag_name}_is_run', _DAG_IS_RUN)))

    self.dag_enable_run_report = bool(
        int(
            Variable.get(f'{self.dag_name}_enable_run_report',
                                  _ENABLE_RETURN_REPORT)))

    self.dag_enable_monitoring = bool(
        int(
            Variable.get(f'{self.dag_name}_enable_monitoring',
                                  _DAG_ENABLE_MONITORING)))
    self.dag_enable_monitoring_cleanup = bool(
        int(
            Variable.get(f'{self.dag_name}_enable_monitoring_cleanup',
                                  _DEFAULT_DAG_ENABLE_MONITORING_CLEANUP)))

    self.days_to_live = int(
        Variable.get(f'{self.dag_name}_days_to_live',
                              _DEFAULT_MONITORING_DATA_DAYS_TO_LIVE))
    self.monitoring_dataset = Variable.get(
        f'{self.dag_name}_monitoring_dataset',
        _DEFAULT_MONITORING_DATASET_ID)
    self.monitoring_table = Variable.get(
        f'{self.dag_name}_monitoring_table', _DEFAULT_MONITORING_TABLE_ID)
    self.monitoring_bq_conn_id = Variable.get(
        f'{self.dag_name}_monitoring_bq_conn_id', _MONITORING_BQ_CONN_ID)

  @abc.abstractmethod
  def create_task(self, main_dag: DAG, is_retry: bool) -> BaseOperator:
    """Creates a task in the given DAG.

    Args:
      main_dag: The DAG that tasks attach to.
      is_retry: Whether the task is a retry or a run.

    Returns:
      A task that has been attached to the DAG.
    """

  def create_dag(self) -> DAG:
    """Creates a DAG.

    Returns:
      A DAG instance.
    """
    main_dag = self._initialize_dag()

    try:
      retry_task = None
      if self.dag_is_retry:
        retry_task = self._try_create_task(main_dag=main_dag, is_retry=True)
      if self.dag_is_run:
        run_task = self._try_create_task(main_dag=main_dag, is_retry=False)
        if self.dag_is_retry:
          run_task.set_upstream(retry_task)
        if self.dag_enable_monitoring_cleanup:
          cleanup_task = self._create_cleanup_task(main_dag=main_dag)
          if self.dag_is_retry:
            cleanup_task >> retry_task
          else:
            cleanup_task >> run_task
    except errors.DAGError as error:
      main_dag = self._initialize_dag()
      create_error_report_task(error_report_dag=main_dag, error=error)

    return main_dag

  def get_task_id(self, task_name: str, is_retry: bool) -> str:
    """Gets task_id by task type.

    Args:
      task_name: The name of the task.
      is_retry: Whether or not the operator should include a retry task.

    Returns:
      Task id.
    """
    if is_retry:
      return task_name + '_retry_task'
    else:
      return task_name + '_task'

  def _initialize_dag(self) -> DAG:
    """Initializes a DAG.

    Returns:
      A DAG instance.
    """
    return DAG(
        self.dag_name,
        default_args={
            'retries': self.dag_retries,
            'retry_delay': datetime.timedelta(minutes=self.dag_retry_delay),
            'start_date': utils.dates.days_ago(1),
        },
        schedule_interval=self.dag_schedule,
        catchup=False)

  def _try_create_task(
      self, main_dag: DAG, is_retry: bool) -> Optional[BaseOperator]:
    """Attempts to create a task.

    If it fails, it creates an error task.

    Args:
      main_dag: The DAG that tasks attach to.
      is_retry: Whether the task is a retry or a run.

    Returns:
      A task.

    Raises:
      DAGError: If task creation fails.
    """
    try:
      return self.create_task(main_dag=main_dag, is_retry=is_retry)
    except Exception as e:  # pylint: disable=broad-except
      logging.exception('Error creating task in dag %s', self.dag_name)
      raise errors.DAGError(f'Error creating task in dag {self.dag_name}') from e

  def _create_cleanup_task(self, main_dag: DAG) -> BaseOperator:
    """Creates a cleanup task.

    Args:
      main_dag: The DAG that tasks attach to.

    Returns:
      A cleanup task.
    """
    return monitoring_cleanup_operator.MonitoringCleanupOperator(
        task_id='cleanup',
        monitoring_dataset_id=self.monitoring_dataset,
        monitoring_table_id=self.monitoring_table,
        bq_conn_id=self.monitoring_bq_conn_id,
        days_to_live=self.days_to_live,
        dag=main_dag)
  def get_variable_value(self,
                         prefix: str,
                         variable_name: str,
                         expected_type: Any = str,
                         fallback_value: object = '',
                         var_not_found_flag: Any = None) -> Any:
    """Try to get value by the prefixed name first, then the name directly.

    Args:
      prefix: The prefix of the variable.
      variable_name: The name of the variable.
      expected_type: The expected type of the value, can be str, int and bool.
      fallback_value: The default value if no such variable is found.
      var_not_found_flag: The flag that indicates no value is found by prefixed
        variable_name and should try to retrieve value by variable_name only.
    Returns:
      The value of expected type of the corresponding variable.
    """
    if fallback_value is not None:
      if not isinstance(fallback_value, expected_type):
        raise TypeError(
            f'type of fallback mismatch expected type {expected_type}')

    val = variable.Variable.get(
        f'{prefix}_{variable_name}', var_not_found_flag)
    if val == var_not_found_flag:
      val = variable.Variable.get(f'{variable_name}', fallback_value)

    try:
      return expected_type(val)
    except ValueError:
      return fallback_value