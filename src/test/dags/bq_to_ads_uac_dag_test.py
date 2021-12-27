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

"""Tests for dags.bq_to_ads_uac_dag."""

import unittest

from dags import bq_to_ads_uac_dag
import dag_test


class DAGTest(dag_test.DAGTest):

  def setUp(self):
    super(DAGTest, self).setUp()
    self.init_airflow_variables(bq_to_ads_uac_dag._DAG_NAME)

  def test_create_dag(self):
    """Tests that returned DAG contains correct DAG and tasks."""
    expected_task_ids = ['bq_to_ads_uac_retry_task', 'bq_to_ads_uac_task']

    is_var_prefixed = False
    test_dag = bq_to_ads_uac_dag.BigQueryToAdsUACDag(
        self.airflow_variables['dag_name']).create_dag()
    self.verify_created_tasks(test_dag, expected_task_ids)
    self.verify_bq_hook(test_dag.tasks[1].input_hook, is_var_prefixed)

    self.add_prefixed_airflow_variables()
    is_var_prefixed = True
    test_dag = bq_to_ads_uac_dag.BigQueryToAdsUACDag(
        self.airflow_variables['dag_name']).create_dag()
    self.verify_created_tasks(test_dag, expected_task_ids)
    self.verify_bq_hook(test_dag.tasks[1].input_hook, is_var_prefixed)


if __name__ == '__main__':
  unittest.main()
