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

"""Tests for dags.gcs_to_cm_dag."""

import unittest

import dag_test
from dags import gcs_to_cm_dag


class GCSToCMDAGTest(dag_test.DAGTest):

  def setUp(self):
    super(GCSToCMDAGTest, self).setUp()
    self.init_airflow_variables(gcs_to_cm_dag._DAG_NAME)

  def test_create_dag(self):
    """Tests that returned DAG contains correct DAG and tasks."""
    expected_task_ids = ['gcs_to_cm_retry_task', 'gcs_to_cm_task']

    is_var_prefixed = False
    test_dag = gcs_to_cm_dag.GCSToCMDag(
        self.airflow_variables['dag_name']).create_dag()
    self.verify_created_tasks(test_dag, expected_task_ids)
    self.verify_gcs_hook(test_dag.tasks[1].input_hook, is_var_prefixed)
    self.verify_cm_hook(test_dag.tasks[1].output_hook, is_var_prefixed)

    self.add_prefixed_airflow_variables()
    is_var_prefixed = True

    test_dag = gcs_to_cm_dag.GCSToCMDag(
        self.airflow_variables['dag_name']).create_dag()
    self.verify_created_tasks(test_dag, expected_task_ids)
    self.verify_gcs_hook(test_dag.tasks[1].input_hook, is_var_prefixed)
    self.verify_cm_hook(test_dag.tasks[1].output_hook, is_var_prefixed)


if __name__ == '__main__':
  unittest.main()
