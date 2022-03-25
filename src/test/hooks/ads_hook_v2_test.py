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

"""Tests for plugins.pipeline_plugins.hooks.ads_hook_v2."""
import json
from typing import Any, Callable, Dict, List, Type
import unittest

import requests
import requests_mock

from plugins.pipeline_plugins.hooks import ads_hook_v2
from plugins.pipeline_plugins.utils import errors


_GOOGLE_ADS_API_BASE = 'https://googleads.googleapis.com/v10'

# Individual Ads account customer ID
_MOCK_CUSTOMER_ID = '1234567890'

# MCC customer ID
_MOCK_LOGIN_CUSTOMER_ID = '800700600'

_MOCK_CLIENT_ID = 'test.apps.googleusercontent.com'

_MOCK_CLIENT_SECRET = 'ABCDEF-abcdefghijklmnopqrstuvyz12'

_MOCK_REFRESH_TOKEN = '1//1234-1234-12345678-123456789'

_MOCK_DEVELOPER_TOKEN = 'abcdefghijklmnopqrstuv'

_MOCK_GOOGLE_ADS_YAML_CREDENTIALS = (
    f'developer_token: {_MOCK_DEVELOPER_TOKEN}\n'
    f'client_id: {_MOCK_CLIENT_ID}\n'
    f'client_secret: {_MOCK_CLIENT_SECRET}\n'
    f'refresh_token: {_MOCK_REFRESH_TOKEN}\n'
    f'login_customer_id: {_MOCK_LOGIN_CUSTOMER_ID}\n')

_MOCK_AUTH_HEADER = {
    'authorization': 'Bearer token',
    'developer-token': _MOCK_DEVELOPER_TOKEN,
    'login-customer-id': _MOCK_LOGIN_CUSTOMER_ID
}

_MOCK_USER_LIST_NAME = 'user_list_name'

_MOCK_APP_ID = 'app_id'

_MOCK_USER_LIST_ID = '9876543210'

_MOCK_OFFLINE_USER_DATA_JOB_ID = '11000000000'

# Mock response is defined as tuple: (json_content, status_code)
_MOCK_200_TOKEN_RESPONSE = ({'access_token': 'token'}, 200)

_MOCK_401_TOKEN_RESPONSE = ({'error': {
    'code': 401,
    'details': [{
        'detail': ('[ORIGINAL ERROR] generic::unauthenticated: Authentication '
                   'of the request failed.')
    }]
}}, 401)

_MOCK_200_SEARCH_USER_LIST_EXIST_RESPONSE = ({
    'results': [{
        'userList': {
            'resourceName': (f'customers/{_MOCK_CUSTOMER_ID}/'
                             f'userLists/{_MOCK_USER_LIST_ID}')
        }
    }],
    'fieldMask': 'userList.resourceName'
}, 200)

_MOCK_200_SEARCH_USER_LIST_NOT_EXIST_RESPONSE = ({
    'fieldMask': 'userList.resourceName'
}, 200)

_MOCK_OFFLINE_USER_DATA_JOB_RESOURCE = (
    f'customers/{_MOCK_CUSTOMER_ID}/'
    f'offlineUserDataJobs/{_MOCK_OFFLINE_USER_DATA_JOB_ID}')

_MOCK_200_SEARCH_OFFLINE_USER_DATA_JOB_STATUS_RESPONSE = ({
    'results': [{
        'offlineUserDataJob': {
            'resourceName': _MOCK_OFFLINE_USER_DATA_JOB_RESOURCE,
            'type': 'CUSTOMER_MATCH_USER_LIST',
            'status': 'RUNNING',
            'id': _MOCK_OFFLINE_USER_DATA_JOB_ID
        }
    }]
}, 200)

_MOCK_200_UPLOAD_CLICK_CONVERSIONS = ({
    'results': []
}, 200)

_MOCK_200_PARTIAL_FAILURE = ({
    'partialFailureError': {
        'code': 3,
        'message': 'message',
        'details': [{
            'errors': [{
                'errorCode': {
                    'internalError': 'INTERNAL_ERROR'
                },
                'location': {
                    'fieldPathElements': [{
                        'fieldName': 'unknown',
                        'index': 0
                    }]
                }
            }],
        }]
    }
}, 200)

_MOCK_200_USER_LIST_CREATED = ({
    'results': [
        {
            'resourceName': (f'customers/{_MOCK_CUSTOMER_ID}/'
                             f'userLists/{_MOCK_USER_LIST_ID}')
        }
    ]
}, 200)

_MOCK_200_OFFLINE_USER_DATA_JOB_CREATED = ({
    'resourceName': _MOCK_OFFLINE_USER_DATA_JOB_RESOURCE
}, 200)

_MOCK_400_BAD_REQUEST = ({
    'error': {
        'code': 400,
        'message': 'bad request'
    }
}, 400)

_MOCK_401_UNAUTHORIZED = ({
    'error': {
        'code': 401,
        'details': [{'detail': 'Token expired.'}]
    }
}, 401)

_MOCK_401_UNAUTHORIZED_2 = ({
    'error': {
        'code': 401,
        'details': [{'detail': 'invalid developer token'}]
    }
}, 401)

_MOCK_SEARCH_USER_LIST_REQUEST = {
    'query': 'SELECT user_list.resource_name FROM user_list '
             f"WHERE user_list.name='{_MOCK_USER_LIST_NAME}' "
             f"AND customer.id = {_MOCK_CUSTOMER_ID} "
             'LIMIT 1'
}

_MOCK_SEARCH_OFFLINE_USER_DATA_JOB_STATUS = {
    'query': (f'SELECT '
              'offline_user_data_job.resource_name,'
              'offline_user_data_job.id,'
              'offline_user_data_job.status,'
              'offline_user_data_job.type,'
              'offline_user_data_job.failure_reason '
              'FROM offline_user_data_job '
              'WHERE offline_user_data_job.resource_name='
              f"'{_MOCK_OFFLINE_USER_DATA_JOB_RESOURCE}' "
              'LIMIT 1')
}


class MockerFacade:

  def __init__(self):
    self.mocker = requests_mock.Mocker()

  def setup_token_url(self, mock_type: str = 'access_token'):
    """Set up the mock token endpoint."""

    # The url in the HTTP request is matched with below url.
    expected_url = ('https://www.googleapis.com/oauth2/v3/token'
                    '?grant_type=refresh_token'
                    f'&client_id={_MOCK_CLIENT_ID}'
                    f'&client_secret={_MOCK_CLIENT_SECRET}'
                    f'&refresh_token={_MOCK_REFRESH_TOKEN}')

    if mock_type == 'access_token':
      self._call_register_uri(
          url=expected_url,
          response_list=self._create_response_list([_MOCK_200_TOKEN_RESPONSE]))
    elif mock_type == 'invalid_oauth2_credential':
      self._call_register_uri(
          url=expected_url,
          response_list=self._create_response_list([_MOCK_401_TOKEN_RESPONSE]))
    else:
      raise ValueError(f'mock_type: {mock_type} is not defined.')

  def setup_upload_click_conversions_url(
      self,
      exc: Type[Exception] = None,
      expected_request_params: Dict[str, Any] = None,
      response_list: List[Any] = None) -> None:
    """Set up the mock upload_click_conversions endpoint."""

    expected_url = (f'{_GOOGLE_ADS_API_BASE}/customers/'
                    f'{_MOCK_CUSTOMER_ID}:uploadClickConversions')

    if expected_request_params:
      # Matching the params in request with expected params.
      def additional_matcher(request):
        actual_request_params = json.loads(request.text)
        return actual_request_params == expected_request_params
    else:
      additional_matcher = None

    self._call_register_uri(
        url=expected_url,
        response_list=self._create_response_list(response_list),
        request_headers=_MOCK_AUTH_HEADER,  # expected HTTP header.
        exc=exc,  # supposes to raise exception.
        additional_matcher=additional_matcher)

  def setup_search_url(self, mock_type: str):
    """Set up the mock search endpoint."""
    expected_url = (f'{_GOOGLE_ADS_API_BASE}/customers/'
                    f'{_MOCK_CUSTOMER_ID}/googleAds:search')

    if mock_type == 'user_list_exist':
      response_list = [_MOCK_200_SEARCH_USER_LIST_EXIST_RESPONSE]
    elif mock_type == 'user_list_not_exist':
      response_list = [_MOCK_200_SEARCH_USER_LIST_NOT_EXIST_RESPONSE]
    else:
      raise ValueError(f'mock_type: {mock_type} is not defined.')
    response_list.append(_MOCK_200_SEARCH_OFFLINE_USER_DATA_JOB_STATUS_RESPONSE)

    def additional_matcher(request):
      actual_request_params = json.loads(request.text)
      if 'user_list' in actual_request_params['query']:
        return actual_request_params == _MOCK_SEARCH_USER_LIST_REQUEST
      else:
        return (actual_request_params ==
                _MOCK_SEARCH_OFFLINE_USER_DATA_JOB_STATUS)

    self._call_register_uri(
        expected_url,
        response_list=self._create_response_list(response_list),
        request_headers=_MOCK_AUTH_HEADER,
        additional_matcher=additional_matcher)

  def setup_user_list_mutate_url(self):
    """Set up the mock user list mutate endpoint."""
    expected_url = (
        f'{_GOOGLE_ADS_API_BASE}/customers/'
        f'{_MOCK_CUSTOMER_ID}/userLists:mutate')

    expected_request_params = {
        'operations': [{
            'create': {
                'name': _MOCK_USER_LIST_NAME,
                'membershipLifeSpan': 30,
                'crmBasedUserList': {
                    'uploadKeyType': 'MOBILE_ADVERTISING_ID',
                    'app_id': 'app_id'
                }
            }
        }],
        'partialFailure': False
    }

    def additional_matcher(request):
      actual_request_params = json.loads(request.text)
      return actual_request_params == expected_request_params

    self._call_register_uri(
        expected_url,
        response_list=self._create_response_list([_MOCK_200_USER_LIST_CREATED]),
        request_headers=_MOCK_AUTH_HEADER,
        additional_matcher=additional_matcher)

  def setup_offline_user_data_jobs_create_url(self):
    """Set up the mock user list mutate endpoint."""
    expected_url = (
        f'{_GOOGLE_ADS_API_BASE}/customers/'
        f'{_MOCK_CUSTOMER_ID}/offlineUserDataJobs:create'
    )

    expected_request_params = {
        'job': {
            'type': 'CUSTOMER_MATCH_USER_LIST',
            'customerMatchUserListMetadata': {
                'userList': (f'customers/{_MOCK_CUSTOMER_ID}/'
                             f'userLists/{_MOCK_USER_LIST_ID}')
            }
        }
    }
    def additional_matcher(request):
      actual_request_params = json.loads(request.text)
      return actual_request_params == expected_request_params

    self._call_register_uri(
        expected_url,
        response_list=self._create_response_list([
            _MOCK_200_OFFLINE_USER_DATA_JOB_CREATED]),
        request_headers=_MOCK_AUTH_HEADER,
        additional_matcher=additional_matcher)

  def setup_offline_user_data_jobs_add_operation(self, mock_type: str):
    expected_url = (f'{_GOOGLE_ADS_API_BASE}/customers/'
                    f'{_MOCK_CUSTOMER_ID}/offlineUserDataJobs/'
                    f'{_MOCK_OFFLINE_USER_DATA_JOB_ID}:addOperations')

    if mock_type == 'hashedEmail':
      user_identifier = {'hashedEmail': 'hashedEmail'}
    elif mock_type == 'hashedPhoneNumber':
      user_identifier = {'hashedPhoneNumber': 'hashedPhoneNumber'}
    elif mock_type == 'addressInfo':
      user_identifier = {
          'addressInfo': {
              'hashedFirstName': 'hashedFirstName',
              'hashedLastName': 'hashedLastName',
              'countryCode': 'countryCode',
              'postalCode': 'postalCode'
          }
      }
    elif mock_type == 'mobileId':
      user_identifier = {'mobileId': 'mobileId'}
    elif mock_type == 'thirdPartyUserId':
      user_identifier = {'thirdPartyUserId': 'thirdPartyUserId'}
    else:
      raise ValueError(f'request_type: {mock_type} is not defined.')

    expected_request_params = {
        'operations': [{
            'create': {
                'userIdentifiers': [user_identifier]
            }
        }],
        'enablePartialFailure': True,
        'enableWarnings': True
    }

    def additional_matcher(request):
      actual_request_params = json.loads(request.text)
      return actual_request_params == expected_request_params

    self._call_register_uri(
        expected_url,
        response_list=self._create_response_list([({}, 200)]),
        request_headers=_MOCK_AUTH_HEADER,
        additional_matcher=additional_matcher)

  def setup_offline_user_data_jobs_run_url(self):
    expected_url = (
        f'{_GOOGLE_ADS_API_BASE}/customers/'
        f'{_MOCK_CUSTOMER_ID}/offlineUserDataJobs/'
        f'{_MOCK_OFFLINE_USER_DATA_JOB_ID}:run')

    expected_request_params = {}

    def additional_matcher(request):
      actual_request_params = json.loads(request.text)
      return actual_request_params == expected_request_params

    self._call_register_uri(
        expected_url,
        response_list=self._create_response_list([({}, 200)]),
        request_headers=_MOCK_AUTH_HEADER,
        additional_matcher=additional_matcher)

  def _call_register_uri(
      self,
      url: str,
      method: str = 'POST',
      response_list: List[Any] = None,
      request_headers: Dict[str, Any] = None,
      additional_matcher: Callable[[Any], bool] = None,
      exc: Exception = None):
    """Register an uri with expected behaviour."""
    if exc:
      self.mocker.register_uri(method=method, url=url, exc=exc)
    else:
      args = {}
      if request_headers:
        args['request_headers'] = request_headers
      if additional_matcher:
        args['additional_matcher'] = additional_matcher

      self.mocker.register_uri(
          method=method,
          url=url,
          response_list=response_list,
          **args)

  def _create_response_list(self, raw_response_list):
    """Converts response definitions to responses in request_mock format."""
    if not raw_response_list:
      return None

    response_list = []
    for text, code in raw_response_list:
      response_list.append({
          'text': json.dumps(text) if isinstance(text, Dict) else text,
          'status_code': int(code)
      })
    return response_list


class GoogleAdsHookV2Test(unittest.TestCase):

  def setUp(self):
    super().setUp()

    self.hook = ads_hook_v2.GoogleAdsHook(
        google_ads_yaml_credentials=_MOCK_GOOGLE_ADS_YAML_CREDENTIALS)

  def test_invalid_google_ads_yaml_credential(self):
    # Assert
    with self.assertRaises(errors.DataOutConnectorAuthenticationError):
      # Arrange / Act
      ads_hook_v2.GoogleAdsHook(google_ads_yaml_credentials='bad_yaml')

  def test_upload_click_conversions(self):
    # Arrange
    payload = {
        'conversionActionId': '987654321',
        'gclid': 'testGclid',
        'conversionDateTime': '2022-01-27 18:00:00+09:00',
        'conversionValue': '0.4'
    }
    payloads = [(0, payload)]

    expected_request_params = {
        'conversions': [{
            'conversionAction': (f'customers/{_MOCK_CUSTOMER_ID}'
                                 f'/conversionActions/987654321'),
            'gclid': 'testGclid',
            'conversionDateTime': '2022-01-27 18:00:00+09:00',
            'currencyCode': 'JPY',
            'conversionValue': '0.4'
        }],
        'partialFailure': True
    }

    facade = MockerFacade()
    facade.setup_token_url()
    facade.setup_upload_click_conversions_url(
        expected_request_params=expected_request_params,
        response_list=[_MOCK_200_UPLOAD_CLICK_CONVERSIONS])

    # Assert
    with facade.mocker:
      # Act
      index_error = self.hook.upload_click_conversions(
          customer_id=_MOCK_CUSTOMER_ID,
          payloads=payloads)
    self.assertListEqual([], index_error)
    self.assertEqual(2, facade.mocker.call_count)

  def test_upload_click_conversions_failed_due_to_bad_oauth2_credential(self):
    # Arrange
    facade = MockerFacade()
    facade.setup_token_url(mock_type='invalid_oauth2_credential')

    with facade.mocker:
      # Assert
      with self.assertRaises(errors.DataOutConnectorError):
        # Act
        self.hook.upload_click_conversions(
            customer_id=_MOCK_CUSTOMER_ID,
            payloads=[])

  def test_upload_click_conversions_failed_with_bad_json_in_response(self):
    # Arrange
    facade = MockerFacade()
    facade.setup_token_url()
    facade.setup_upload_click_conversions_url(
        response_list=[('bad JSON', 200)])

    with facade.mocker:
      # Assert
      with self.assertRaises(errors.DataOutConnectorError):
        # Act
        self.hook.upload_click_conversions(
            customer_id=_MOCK_CUSTOMER_ID,
            payloads=[])

  def test_upload_click_conversions_failed_due_to_bad_developer_token(self):
    # Arrange
    # Bad developer token causes 401 error.
    payload = {
        'conversionActionId': '987654321',
        'gclid': 'testGclid',
        'conversionDateTime': '2022-01-27 18:00:00+09:00'
    }
    payloads = [(0, payload)]

    expected_request_params = {
        'conversions': [{
            'conversionAction': (f'customers/{_MOCK_CUSTOMER_ID}'
                                 f'/conversionActions/987654321'),
            'gclid': 'testGclid',
            'conversionDateTime': '2022-01-27 18:00:00+09:00',
            'currencyCode': 'JPY'
        }],
        'partialFailure': True
    }

    facade = MockerFacade()
    facade.setup_token_url()
    facade.setup_upload_click_conversions_url(
        expected_request_params=expected_request_params,
        response_list=[_MOCK_400_BAD_REQUEST])

    # Assert
    with self.assertRaises(errors.DataOutConnectorError):
      with facade.mocker:
        # Act
        self.hook.upload_click_conversions(
            customer_id=_MOCK_CUSTOMER_ID,
            payloads=payloads)
    self.assertEqual(2, facade.mocker.call_count)

  def test_upload_click_conversions_encounters_internal_error(self):
    # Arrange
    facade = MockerFacade()
    facade.setup_token_url()
    # Expects the API call raises an exception categorized as retriable error.
    facade.setup_upload_click_conversions_url(exc=requests.ConnectTimeout)

    with facade.mocker:
      # Assert
      with self.assertRaises(errors.DataOutConnectorSendUnsuccessfulError):
        # Act
        self.hook.upload_click_conversions(
            customer_id=_MOCK_CUSTOMER_ID,
            payloads=[])
      self.assertEqual(4, facade.mocker.call_count)

  def test_upload_click_conversions_encounters_non_internal_error(self):
    # Arrange
    # Missing conversionValue field causes 400 error.
    payload = {
        'conversionActionId': '987654321',
        'gclid': 'testGclid',
        'conversionDateTime': '2022-01-27 18:00:00+09:00'
    }
    payloads = [(0, payload)]

    expected_request_params = {
        'conversions': [{
            'conversionAction': (f'customers/{_MOCK_CUSTOMER_ID}'
                                 f'/conversionActions/987654321'),
            'gclid': 'testGclid',
            'conversionDateTime': '2022-01-27 18:00:00+09:00',
            'currencyCode': 'JPY'
        }],
        'partialFailure': True
    }

    facade = MockerFacade()
    facade.setup_token_url()
    facade.setup_upload_click_conversions_url(
        expected_request_params=expected_request_params,
        response_list=[_MOCK_401_UNAUTHORIZED_2])

    # Assert
    with self.assertRaises(errors.DataOutConnectorAuthenticationError):
      with facade.mocker:
        # Act
        self.hook.upload_click_conversions(
            customer_id=_MOCK_CUSTOMER_ID,
            payloads=payloads)
    self.assertEqual(2, facade.mocker.call_count)

  def test_upload_click_conversions_encounters_access_token_expired(self):
    payload = {
        'conversionActionId': '987654321',
        'gclid': 'testGclid',
        'conversionDateTime': '2022-01-27 18:00:00+09:00'
    }
    payloads = [(0, payload)]

    expected_request_params = {
        'conversions': [{
            'conversionAction': (f'customers/{_MOCK_CUSTOMER_ID}'
                                 f'/conversionActions/987654321'),
            'gclid': 'testGclid',
            'conversionDateTime': '2022-01-27 18:00:00+09:00',
            'currencyCode': 'JPY'
        }],
        'partialFailure': True
    }

    facade = MockerFacade()
    facade.setup_token_url()
    facade.setup_upload_click_conversions_url(
        response_list=[_MOCK_401_UNAUTHORIZED, _MOCK_200_PARTIAL_FAILURE],
        expected_request_params=expected_request_params)

    with facade.mocker:
      index_error = self.hook.upload_click_conversions(
          customer_id=_MOCK_CUSTOMER_ID,
          payloads=payloads)
      self.assertListEqual(
          index_error,
          [(0, errors.ErrorNameIDMap.RETRIABLE_ERROR_EVENT_NOT_SENT)])
      self.assertEqual(4, facade.mocker.call_count)

  def test_upload_customer_match_user_list_with_empty_user_list_name(self):
    with self.assertRaises(errors.DataOutConnectorValueError):
      self.hook.upload_customer_match_user_list(
          customer_id=_MOCK_CUSTOMER_ID,
          user_list_name='',
          create_new_list=False,
          payloads=[],
          upload_key_type=ads_hook_v2.CUSTOMER_MATCH_UPLOAD_KEY_CONTACT_INFO)

  def test_upload_customer_match_user_list_with_empty_app_id(self):
    with self.assertRaises(errors.DataOutConnectorValueError):
      self.hook.upload_customer_match_user_list(
          customer_id=_MOCK_CUSTOMER_ID,
          user_list_name=_MOCK_USER_LIST_NAME,
          create_new_list=True,
          payloads=[],
          upload_key_type=ads_hook_v2
          .CUSTOMER_MATCH_UPLOAD_KEY_MOBILE_ADVERTISING_ID,
          app_id='')

  def test_upload_customer_match_user_list_with_create_new_list_false(self):
    facade = MockerFacade()
    facade.setup_token_url()
    facade.setup_search_url(mock_type='user_list_not_exist')
    with facade.mocker:
      with self.assertRaises(errors.DataOutConnectorValueError):
        self.hook.upload_customer_match_user_list(
            customer_id=_MOCK_CUSTOMER_ID,
            user_list_name=_MOCK_USER_LIST_NAME,
            create_new_list=False,
            payloads=[],
            upload_key_type=ads_hook_v2.CUSTOMER_MATCH_UPLOAD_KEY_CRM_ID)
      self.assertEqual(2, facade.mocker.call_count)

  def test_upload_customer_match_user_list_contact_info_hashed_email(self):
    payload = {
        'hashedEmail': 'hashedEmail'
    }
    payloads = [(0, payload)]

    facade = MockerFacade()
    facade.setup_token_url()
    facade.setup_search_url(mock_type='user_list_exist')
    facade.setup_offline_user_data_jobs_create_url()
    facade.setup_offline_user_data_jobs_add_operation(mock_type='hashedEmail')
    facade.setup_offline_user_data_jobs_run_url()
    with facade.mocker:
      self.hook.upload_customer_match_user_list(
          customer_id=_MOCK_CUSTOMER_ID,
          user_list_name=_MOCK_USER_LIST_NAME,
          create_new_list=False,
          payloads=payloads,
          upload_key_type=ads_hook_v2.CUSTOMER_MATCH_UPLOAD_KEY_CONTACT_INFO)
      self.assertEqual(6, facade.mocker.call_count)

  def test_upload_customer_match_user_list_contact_info_hashed_phone_num(self):
    payload = {
        'hashedPhoneNumber': 'hashedPhoneNumber'
    }
    payloads = [(0, payload)]

    facade = MockerFacade()
    facade.setup_token_url()
    facade.setup_search_url(mock_type='user_list_exist')
    facade.setup_offline_user_data_jobs_create_url()
    facade.setup_offline_user_data_jobs_add_operation(
        mock_type='hashedPhoneNumber')
    facade.setup_offline_user_data_jobs_run_url()
    with facade.mocker:
      self.hook.upload_customer_match_user_list(
          customer_id=_MOCK_CUSTOMER_ID,
          user_list_name=_MOCK_USER_LIST_NAME,
          create_new_list=False,
          payloads=payloads,
          upload_key_type=ads_hook_v2.CUSTOMER_MATCH_UPLOAD_KEY_CONTACT_INFO)
      self.assertEqual(6, facade.mocker.call_count)

  def test_upload_customer_match_user_list_contact_info_address_info(self):
    payload = {
        'hashedFirstName': 'hashedFirstName',
        'hashedLastName': 'hashedLastName',
        'countryCode': 'countryCode',
        'postalCode': 'postalCode'
    }
    payloads = [(0, payload)]

    facade = MockerFacade()
    facade.setup_token_url()
    facade.setup_search_url(mock_type='user_list_exist')
    facade.setup_offline_user_data_jobs_create_url()
    facade.setup_offline_user_data_jobs_add_operation(mock_type='addressInfo')
    facade.setup_offline_user_data_jobs_run_url()
    with facade.mocker:
      self.hook.upload_customer_match_user_list(
          customer_id=_MOCK_CUSTOMER_ID,
          user_list_name=_MOCK_USER_LIST_NAME,
          create_new_list=False,
          payloads=payloads,
          upload_key_type=ads_hook_v2.CUSTOMER_MATCH_UPLOAD_KEY_CONTACT_INFO)
      self.assertEqual(6, facade.mocker.call_count)

  def test_upload_customer_match_user_list_mobile_id_to_new_list(self):
    payload = {
        'mobileId': 'mobileId'
    }
    payloads = [(0, payload)]

    facade = MockerFacade()
    facade.setup_token_url()
    facade.setup_search_url(mock_type='user_list_not_exist')
    facade.setup_user_list_mutate_url()
    facade.setup_offline_user_data_jobs_create_url()
    facade.setup_offline_user_data_jobs_add_operation(mock_type='mobileId')
    facade.setup_offline_user_data_jobs_run_url()
    with facade.mocker:
      self.hook.upload_customer_match_user_list(
          customer_id=_MOCK_CUSTOMER_ID,
          user_list_name=_MOCK_USER_LIST_NAME,
          create_new_list=True,
          payloads=payloads,
          upload_key_type=ads_hook_v2
          .CUSTOMER_MATCH_UPLOAD_KEY_MOBILE_ADVERTISING_ID,
          app_id=_MOCK_APP_ID,
          membership_life_span_days=30)
      self.assertEqual(7, facade.mocker.call_count)

  def test_upload_customer_match_user_list_crm_id(self):
    payload = {
        'thirdPartyUserId': 'thirdPartyUserId'
    }
    payloads = [(0, payload)]

    facade = MockerFacade()
    facade.setup_token_url()
    facade.setup_search_url(mock_type='user_list_exist')
    facade.setup_offline_user_data_jobs_create_url()
    facade.setup_offline_user_data_jobs_add_operation(
        mock_type='thirdPartyUserId')
    facade.setup_offline_user_data_jobs_run_url()
    with facade.mocker:
      self.hook.upload_customer_match_user_list(
          customer_id=_MOCK_CUSTOMER_ID,
          user_list_name=_MOCK_USER_LIST_NAME,
          create_new_list=False,
          payloads=payloads,
          upload_key_type=ads_hook_v2.CUSTOMER_MATCH_UPLOAD_KEY_CRM_ID)
      self.assertEqual(6, facade.mocker.call_count)

if __name__ == '__main__':
  unittest.main()
