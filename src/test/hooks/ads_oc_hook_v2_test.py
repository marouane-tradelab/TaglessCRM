# python3
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

"""Tests for plugins.pipeline_plugins.hooks.ads_oc_v2_hook."""

import unittest
from unittest import mock

from plugins.pipeline_plugins.hooks import ads_hook_v2
from plugins.pipeline_plugins.hooks import ads_oc_hook_v2
from plugins.pipeline_plugins.utils import blob as blob_lib
from plugins.pipeline_plugins.utils import errors


_TEST_EVENT = {
    ads_hook_v2.CUSTOMER_ID: '12345',
    ads_hook_v2.CONVERSION_ACTION_ID: '23456',
    ads_hook_v2.GCLID: '123abc',
    ads_hook_v2.CONVERSION_VALUE: 0.4732,
    ads_hook_v2.CONVERSION_DATE_TIME: '2022-02-10 12:32:40+09:00'
}


class GoogleAdsOfflineConversionsHookTest(unittest.TestCase):

  def setUp(self):
    """Setup function for each unit test."""
    super().setUp()

    mock.patch(('plugins.pipeline_plugins.hooks'
                '.ads_oc_hook_v2.GoogleAdsOfflineConversionsHook.__init__'),
               return_value=None).start()
    self.test_hook = ads_oc_hook_v2.GoogleAdsOfflineConversionsHook(
        google_ads_yaml_credentials='')

    self.upload_click_conversions = (
        mock.patch(('plugins.pipeline_plugins.hooks'
                    '.ads_oc_hook_v2.GoogleAdsOfflineConversionsHook'
                    '.upload_click_conversions')).start())
    self.upload_click_conversions.return_value = []

    self.addCleanup(mock.patch.stopall)

  def test_send_events(self):
    events = [dict(_TEST_EVENT), dict(_TEST_EVENT)]
    events[1][ads_hook_v2.CUSTOMER_ID] = '12346'

    blob = blob_lib.Blob(events=events, location='')
    blob = self.test_hook.send_events(blob)
    calls = [mock.call(events[0][ads_hook_v2.CUSTOMER_ID], [(0, events[0])]),
             mock.call(events[1][ads_hook_v2.CUSTOMER_ID], [(1, events[1])])]
    self.upload_click_conversions.assert_has_calls(calls)
    self.assertEqual(len(blob.failed_events), 0)

  def test_send_events_request_failure(self):
    events = [dict(_TEST_EVENT), dict(_TEST_EVENT)]
    self.upload_click_conversions.side_effect = errors.DataOutConnectorError()
    blob = blob_lib.Blob(events=events, location='', position=1000)
    blob = self.test_hook.send_events(blob)
    self.assertEqual(1, self.upload_click_conversions.call_count)
    self.assertEqual(len(blob.failed_events), 2)
    self.assertTupleEqual(blob.failed_events[0], (1000, _TEST_EVENT, 10))
    self.assertTupleEqual(blob.failed_events[1], (1001, _TEST_EVENT, 10))

  def test_send_events_partial_failure(self):
    events = [dict(_TEST_EVENT), dict(_TEST_EVENT)]
    self.upload_click_conversions.return_value = [
        (0, errors.ErrorNameIDMap.NON_RETRIABLE_ERROR_EVENT_NOT_SENT)
    ]
    blob = blob_lib.Blob(events=events, location='')
    blob = self.test_hook.send_events(blob)
    self.assertEqual(1, self.upload_click_conversions.call_count)
    self.assertEqual(len(blob.failed_events), 1)

if __name__ == '__main__':
  unittest.main()
