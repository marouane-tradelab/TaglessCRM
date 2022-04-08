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

"""Tests for plugins.pipeline_plugins.hooks.ads_cm_v2_hook."""
import unittest
import unittest.mock as mock
from parameterized import parameterized

from plugins.pipeline_plugins.hooks import ads_cm_hook_v2
from plugins.pipeline_plugins.hooks import ads_hook_v2
from plugins.pipeline_plugins.utils import blob as blob_lib
from plugins.pipeline_plugins.utils import errors


_TEST_EVENT = {
    ads_hook_v2.CUSTOMER_ID: '12345',
    ads_hook_v2.THIRD_PARTY_USER_ID: '00000000-1111-2222-3333-444444444444',
}


class GoogleAdsCustomerMatchHookTest(unittest.TestCase):

  def setUp(self):
    """Setup function for each unit test."""
    super().setUp()

    mock.patch(('plugins.pipeline_plugins.hooks'
                '.ads_hook_v2.GoogleAdsHook.__init__'),
               return_value=None).start()
    self.test_hook = ads_cm_hook_v2.GoogleAdsCustomerMatchHook(
        google_ads_yaml_credentials='',
        ads_cm_user_list_name='test',
        ads_upload_key_type=ads_hook_v2.CUSTOMER_MATCH_UPLOAD_KEY_CRM_ID)

    self.upload_customer_match_user_list = (
        mock.patch(('plugins.pipeline_plugins.hooks'
                    '.ads_cm_hook_v2.GoogleAdsCustomerMatchHook'
                    '.upload_customer_match_user_list')).start())
    self.upload_customer_match_user_list.return_value = []

    self.addCleanup(mock.patch.stopall)

  def test_init_empty_user_list_name(self):
    with self.assertRaises(errors.DataOutConnectorValueError):
      ads_cm_hook_v2.GoogleAdsCustomerMatchHook(
          google_ads_yaml_credentials='',
          ads_cm_user_list_name='',
          ads_upload_key_type=ads_hook_v2.CUSTOMER_MATCH_UPLOAD_KEY_CRM_ID)

  @parameterized.expand([(-1,), (541,)])
  def test_init_membership_lifespan_out_of_range(self, membership_lifespan):
    with self.assertRaises(errors.DataOutConnectorValueError):
      ads_cm_hook_v2.GoogleAdsCustomerMatchHook(
          google_ads_yaml_credentials='',
          ads_cm_user_list_name='test',
          ads_upload_key_type=ads_hook_v2.CUSTOMER_MATCH_UPLOAD_KEY_CRM_ID,
          ads_cm_membership_lifespan_in_days=membership_lifespan)

  def test_init_invalid_upload_key_type(self):
    with self.assertRaises(errors.DataOutConnectorValueError):
      ads_cm_hook_v2.GoogleAdsCustomerMatchHook(
          google_ads_yaml_credentials='',
          ads_cm_user_list_name='test',
          ads_upload_key_type='invalid')

  def test_init_create_list_true_no_app_id(self):
    with self.assertRaises(errors.DataOutConnectorValueError):
      ads_cm_hook_v2.GoogleAdsCustomerMatchHook(
          google_ads_yaml_credentials='',
          ads_cm_user_list_name='test',
          ads_upload_key_type=ads_hook_v2
          .CUSTOMER_MATCH_UPLOAD_KEY_MOBILE_ADVERTISING_ID,
          ads_cm_create_list=True,
          ads_cm_app_id='')

  def test_send_events(self):
    events = [dict(_TEST_EVENT), dict(_TEST_EVENT)]
    events[1][ads_hook_v2.CUSTOMER_ID] = '12346'

    blob = blob_lib.Blob(events=events, location='')
    blob = self.test_hook.send_events(blob)

    call_args = {
        'customer_id': events[0][ads_hook_v2.CUSTOMER_ID],
        'user_list_name': self.test_hook.user_list_name,
        'create_new_list': self.test_hook.create_list,
        'payloads': [(0, events[0])],
        'upload_key_type': self.test_hook.upload_key_type,
        'app_id': self.test_hook.app_id,
        'membership_life_span_days': self.test_hook.membership_lifespan
    }
    call_args_2 = dict(call_args)
    call_args_2['customer_id'] = events[1][ads_hook_v2.CUSTOMER_ID]
    call_args_2['payloads'] = [(1, events[1])]

    calls = [mock.call(**call_args),
             mock.call(**call_args_2)]
    self.upload_customer_match_user_list.assert_has_calls(calls)
    self.assertEqual(len(blob.failed_events), 0)

  def test_send_events_request_failure(self):
    events = [dict(_TEST_EVENT), dict(_TEST_EVENT)]
    self.upload_customer_match_user_list.side_effect = (
        errors.DataOutConnectorError())
    blob = blob_lib.Blob(events=events, location='', position=2000)
    blob = self.test_hook.send_events(blob)
    self.assertEqual(1, self.upload_customer_match_user_list.call_count)
    self.assertEqual(len(blob.failed_events), 2)
    self.assertTupleEqual(blob.failed_events[0], (2000, _TEST_EVENT, 10))
    self.assertTupleEqual(blob.failed_events[1], (2001, _TEST_EVENT, 10))


if __name__ == '__main__':
  unittest.main()
