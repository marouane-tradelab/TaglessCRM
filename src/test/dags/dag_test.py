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

import unittest

from airflow.contrib.hooks import bigquery_hook
from airflow.contrib.hooks import gcp_api_base_hook
from airflow.models import dag
from airflow.models import variable
import mock

from gps_building_blocks.cloud.utils import cloud_auth
from plugins.pipeline_plugins.hooks import ads_cm_hook
from plugins.pipeline_plugins.hooks import ads_hook
from plugins.pipeline_plugins.hooks import ads_oc_hook
from plugins.pipeline_plugins.hooks import bq_hook
from plugins.pipeline_plugins.hooks import cm_hook
from plugins.pipeline_plugins.hooks import ga4_hook
from plugins.pipeline_plugins.hooks import ga_hook
from plugins.pipeline_plugins.hooks import gcs_hook
from plugins.pipeline_plugins.hooks import monitoring_hook
from plugins.pipeline_plugins.operators import data_connector_operator

_PREFIX = 'prefix_'
_DATASET_ID = 'test_dataset'
_TABLE_ID = 'test_table'
_PREFIX_DATASET_ID = _PREFIX + _DATASET_ID
_PREFIX_TABLE_ID = _PREFIX + _TABLE_ID
_ADS_CREDENTIALS = (
    'adwords:\n'
    '  client_customer_id: 123-456-7890\n'
    '  developer_token: developer_token\n'
    '  client_id: test.apps.googleusercontent.com\n'
    '  client_secret: secret\n'
    '  refresh_token: 1//token\n')
_PREFIX_ADS_CREDENTIALS = (
    'adwords:\n'
    '  client_customer_id: 123-456-7890\n'
    '  developer_token: prefix_developer_token\n'
    '  client_id: test.apps.googleusercontent.com\n'
    '  client_secret: secret\n'
    '  refresh_token: 1//token\n')
_ADS_UPLOAD_KEY_TYPE = 'CRM_ID'
_PREFIX_ADS_UPLOAD_KEY_TYPE = 'CONTACT_INFO'
_ADS_CM_APP_ID = '1'
_PREFIX_ADS_CM_APP_ID = _PREFIX + _ADS_CM_APP_ID
_ADS_CM_CREATE_LIST = 'True'
_PREFIX_ADS_CM_CREATE_LIST = 'False'
_ADS_CM_MEMBERSHIP_LIFESPAN = '8'
_PREFIX_ADS_CM_MEMBERSHIP_LIFESPAN = '4'
_ADS_CM_USER_LIST_NAME = 'my_list'
_PREFIX_ADS_CM_USER_LIST_NAME = _PREFIX + _ADS_CM_USER_LIST_NAME
_CM_PROFILE_ID = 'cm_profile_id'
_PREFIX_CM_PROFILE_ID = _PREFIX + _CM_PROFILE_ID
_CM_SERVICE_ACCOUNT = 'cm_service_account'
_PREFIX_CM_SERVICE_ACCOUNT = _PREFIX + _CM_SERVICE_ACCOUNT
_API_SECRET = 'test_secret'
_PREFIX_API_SECRET = _PREFIX + _API_SECRET
_PAYLOAD_TYPE = 'gtag'
_PREFIX_PAYLOAD_TYPE = 'firebase'
_MEASUREMENT_ID = 'test_measurement_id'
_PREFIX_MEASUREMENT_ID = _PREFIX + _MEASUREMENT_ID
_FIREBASE_APP_ID = 'test_firebase_app_id'
_PREFIX_FIREBASE_APP_ID = _PREFIX + _FIREBASE_APP_ID
_GA_TRACKING_ID = 'UA-12345-67'
_PREFIX_GA_TRACKING_ID = 'UA-54321-76'
_GCS_BUCKET_NAME = 'test_bucket'
_PREFIX_GCS_BUCKET_NAME = _PREFIX + _GCS_BUCKET_NAME
_GCS_BUCKET_PREFIX = 'test_dataset'
_PREFIX_GCS_BUCKET_PREFIX = _PREFIX + _GCS_BUCKET_PREFIX
_GCS_CONTENT_TYPE = 'JSON'
_PREFIX_GCS_CONTENT_TYPE = 'CSV'


class DAGTest(unittest.TestCase):

  def setUp(self) -> None:
    super(DAGTest, self).setUp()
    self.addCleanup(mock.patch.stopall)
    self.airflow_variables = None
    self.prefixed_variables = None
    self.mock_variable = mock.patch.object(
        variable, 'Variable', autospec=True).start()
    # `side_effect` is assigned to `lambda` to dynamically return values
    # each time when self.mock_variable is called.
    def mock_variable_get(key, default_var):
      if key in self.airflow_variables:
        return self.airflow_variables[key]
      else:
        return default_var
    self.mock_variable.get.side_effect = mock_variable_get

    self.original_gcp_hook_init = gcp_api_base_hook.GoogleCloudBaseHook.__init__
    gcp_api_base_hook.GoogleCloudBaseHook.__init__ = mock.MagicMock()

    self.original_bigquery_hook_init = bigquery_hook.BigQueryHook.__init__
    bigquery_hook.BigQueryHook.__init__ = mock.MagicMock()

    self.original_monitoring_hook = monitoring_hook.MonitoringHook
    monitoring_hook.MonitoringHook = mock.MagicMock()

    self.addCleanup(mock.patch.stopall)
    self.build_impersonated_client_mock = mock.patch.object(
        cloud_auth, 'build_impersonated_client', autospec=True)
    self.build_impersonated_client_mock.return_value = mock.Mock()
    self.build_impersonated_client_mock.start()

  def tearDown(self):
    super().tearDown()
    bigquery_hook.BigQueryHook.__init__ = self.original_bigquery_hook_init
    monitoring_hook.MonitoringHook = self.original_monitoring_hook

  def init_airflow_variables(self, dag_name):
    self.airflow_variables = {
        'dag_name': dag_name,
        f'{dag_name}_schedule': '@once',
        f'{dag_name}_retries': 0,
        f'{dag_name}_retry_delay': 3,
        f'{dag_name}_is_retry': True,
        f'{dag_name}_is_run': True,
        f'{dag_name}_enable_run_report': False,
        f'{dag_name}_enable_monitoring': True,
        f'{dag_name}_enable_monitoring_cleanup': False,
        'monitoring_data_days_to_live': 50,
        'monitoring_dataset': 'test_monitoring_dataset',
        'monitoring_table': 'test_monitoring_table',
        'monitoring_bq_conn_id': 'test_monitoring_conn',
        'bq_dataset_id': _DATASET_ID,
        'bq_table_id': _TABLE_ID,
        'gcs_bucket_name': _GCS_BUCKET_NAME,
        'gcs_bucket_prefix': _GCS_BUCKET_PREFIX,
        'gcs_content_type': _GCS_CONTENT_TYPE,
        'ads_credentials': _ADS_CREDENTIALS,
        'ads_upload_key_type': _ADS_UPLOAD_KEY_TYPE,
        'ads_cm_app_id': _ADS_CM_APP_ID,
        'ads_cm_create_list': _ADS_CM_CREATE_LIST,
        'ads_cm_membership_lifespan': _ADS_CM_MEMBERSHIP_LIFESPAN,
        'ads_cm_user_list_name': _ADS_CM_USER_LIST_NAME,
        'cm_profile_id': _CM_PROFILE_ID,
        'cm_service_account': _CM_SERVICE_ACCOUNT,
        'api_secret': _API_SECRET,
        'payload_type': _PAYLOAD_TYPE,
        'measurement_id': _MEASUREMENT_ID,
        'firebase_app_id': _FIREBASE_APP_ID,
        'ga_tracking_id': _GA_TRACKING_ID
    }
    self.prefixed_variables = {
        f'{dag_name}_bq_dataset_id': _PREFIX_DATASET_ID,
        f'{dag_name}_bq_table_id': _PREFIX_TABLE_ID,
        f'{dag_name}_gcs_bucket_name': _PREFIX_GCS_BUCKET_NAME,
        f'{dag_name}_gcs_bucket_prefix': _PREFIX_GCS_BUCKET_PREFIX,
        f'{dag_name}_gcs_content_type': _PREFIX_GCS_CONTENT_TYPE,
        f'{dag_name}_ads_credentials': _PREFIX_ADS_CREDENTIALS,
        f'{dag_name}_ads_upload_key_type': _PREFIX_ADS_UPLOAD_KEY_TYPE,
        f'{dag_name}_ads_cm_app_id': _PREFIX_ADS_CM_APP_ID,
        f'{dag_name}_ads_cm_create_list': _PREFIX_ADS_CM_CREATE_LIST,
        f'{dag_name}_ads_cm_membership_lifespan':
            _PREFIX_ADS_CM_MEMBERSHIP_LIFESPAN,
        f'{dag_name}_ads_cm_user_list_name': _PREFIX_ADS_CM_USER_LIST_NAME,
        f'{dag_name}_cm_profile_id': _PREFIX_CM_PROFILE_ID,
        f'{dag_name}_cm_service_account': _PREFIX_CM_SERVICE_ACCOUNT,
        f'{dag_name}_api_secret': _PREFIX_API_SECRET,
        f'{dag_name}_payload_type': _PREFIX_PAYLOAD_TYPE,
        f'{dag_name}_measurement_id': _PREFIX_MEASUREMENT_ID,
        f'{dag_name}_firebase_app_id': _PREFIX_FIREBASE_APP_ID,
        f'{dag_name}_ga_tracking_id': _PREFIX_GA_TRACKING_ID
    }

  def add_prefixed_airflow_variables(self):
    self.airflow_variables = {**self.airflow_variables,
                              **self.prefixed_variables}

  def verify_created_tasks(self, test_dag, expected_task_ids):
    self.assertIsInstance(test_dag, dag.DAG)
    self.assertEqual(len(test_dag.tasks), len(expected_task_ids))
    for task in test_dag.tasks:
      self.assertIsInstance(task, data_connector_operator.DataConnectorOperator)
    actual_task_ids = [t.task_id for t in test_dag.tasks]
    self.assertListEqual(actual_task_ids, expected_task_ids)

  def verify_bq_hook(self, expected_hook, is_var_prefixed):
    self.assertIsInstance(expected_hook, bq_hook.BigQueryHook)

    if is_var_prefixed:
      self.assertEqual(expected_hook.dataset_id, _PREFIX_DATASET_ID)
      self.assertEqual(expected_hook.table_id, _PREFIX_TABLE_ID)
    else:
      self.assertEqual(expected_hook.dataset_id, _DATASET_ID)
      self.assertEqual(expected_hook.table_id, _TABLE_ID)

  def verify_gcs_hook(self, expected_hook, is_var_prefixed):
    self.assertIsInstance(expected_hook, gcs_hook.GoogleCloudStorageHook)

    if is_var_prefixed:
      self.assertEqual(expected_hook.bucket, _PREFIX_GCS_BUCKET_NAME)
      self.assertEqual(expected_hook.content_type, _PREFIX_GCS_CONTENT_TYPE)
      self.assertEqual(expected_hook.prefix, _PREFIX_GCS_BUCKET_PREFIX)

    else:
      self.assertEqual(expected_hook.bucket, _GCS_BUCKET_NAME)
      self.assertEqual(expected_hook.content_type, _GCS_CONTENT_TYPE)
      self.assertEqual(expected_hook.prefix, _GCS_BUCKET_PREFIX)

  def verify_ads_oc_hook(self, expected_hook, is_var_prefixed):
    self.assertIsInstance(
        expected_hook, ads_oc_hook.GoogleAdsOfflineConversionsHook)

    if is_var_prefixed:
      self.assertEqual(expected_hook.yaml_doc, _PREFIX_ADS_CREDENTIALS)
    else:
      self.assertEqual(expected_hook.yaml_doc, _ADS_CREDENTIALS)

  def verify_ads_cm_hook(self, expected_hook, is_var_prefixed):
    self.assertIsInstance(expected_hook, ads_cm_hook.GoogleAdsCustomerMatchHook)

    if is_var_prefixed:
      self.assertEqual(
          expected_hook.user_list_name, _PREFIX_ADS_CM_USER_LIST_NAME)
      self.assertEqual(
          expected_hook.upload_key_type,
          ads_hook.UploadKeyType[_PREFIX_ADS_UPLOAD_KEY_TYPE])
      self.assertEqual(expected_hook.yaml_doc, _PREFIX_ADS_CREDENTIALS)
      self.assertEqual(
          expected_hook.membership_lifespan,
          int(_PREFIX_ADS_CM_MEMBERSHIP_LIFESPAN))
      self.assertEqual(
          expected_hook.create_list, bool(_PREFIX_ADS_CM_CREATE_LIST))
    else:
      self.assertEqual(expected_hook.app_id, _ADS_CM_APP_ID)
      self.assertEqual(expected_hook.user_list_name, _ADS_CM_USER_LIST_NAME)
      self.assertEqual(expected_hook.upload_key_type,
                       ads_hook.UploadKeyType[_ADS_UPLOAD_KEY_TYPE])
      self.assertEqual(expected_hook.yaml_doc, _ADS_CREDENTIALS)
      self.assertEqual(
          expected_hook.membership_lifespan, int(_ADS_CM_MEMBERSHIP_LIFESPAN))
      self.assertEqual(expected_hook.create_list, bool(_ADS_CM_CREATE_LIST))
      self.assertEqual(expected_hook.app_id, _ADS_CM_APP_ID)

  def verify_cm_hook(self, expected_hook, is_var_prefixed):
    self.assertIsInstance(expected_hook, cm_hook.CampaignManagerHook)

    self.assertIsNotNone(self.build_impersonated_client_mock.get_original())
    mock_func_call = self.build_impersonated_client_mock.get_original()[0]

    if is_var_prefixed:
      mock_func_call.assert_called_with(
          cm_hook._API_SERVICE,
          _PREFIX_CM_SERVICE_ACCOUNT,
          cm_hook._API_VERSION,
          cm_hook._API_SCOPE)
      self.assertEqual(expected_hook._profile_id, _PREFIX_CM_PROFILE_ID)
    else:
      self.assertEqual(expected_hook._profile_id, _CM_PROFILE_ID)
      mock_func_call.assert_called_with(
          cm_hook._API_SERVICE,
          _CM_SERVICE_ACCOUNT,
          cm_hook._API_VERSION,
          cm_hook._API_SCOPE)

  def verify_ga4_hook(self, expected_hook, is_var_prefixed):
    self.assertIsInstance(expected_hook, ga4_hook.GoogleAnalyticsV4Hook)

    if is_var_prefixed:
      self.assertEqual(expected_hook.api_secret, _PREFIX_API_SECRET)
      self.assertEqual(expected_hook.payload_type, _PREFIX_PAYLOAD_TYPE)
      self.assertEqual(expected_hook.measurement_id, _PREFIX_MEASUREMENT_ID)
      self.assertEqual(expected_hook.firebase_app_id, _PREFIX_FIREBASE_APP_ID)
    else:
      self.assertEqual(expected_hook.api_secret, _API_SECRET)
      self.assertEqual(expected_hook.payload_type, _PAYLOAD_TYPE)
      self.assertEqual(expected_hook.measurement_id, _MEASUREMENT_ID)
      self.assertEqual(expected_hook.firebase_app_id, _FIREBASE_APP_ID)

  def verify_ga_hook(self, expected_hook, is_var_prefixed):
    self.assertIsInstance(expected_hook, ga_hook.GoogleAnalyticsHook)

    if is_var_prefixed:
      self.assertEqual(expected_hook.tracking_id, _PREFIX_GA_TRACKING_ID)
    else:
      self.assertEqual(expected_hook.tracking_id, _GA_TRACKING_ID)
