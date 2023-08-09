from typing import Optional

import base
from pipeline_plugins.hooks import monitoring_hook as monitoring
from pipeline_plugins.utils import hook_factory

class BigQueryToAdsOCOperator(base.BaseDataConnectorOperator):
    def __init__(self,
                *args,
                 dag_name: str,
                 is_retry: bool,
                 return_report: bool,
                 enable_monitoring: bool,
                 monitoring_dataset: str,
                 monitoring_table: str,
                 monitoring_bq_conn_id: str,
                 bq_conn_id: str,
                 bq_dataset_id: str,
                 bq_table_id: str,
                 api_version: str,
                 google_ads_yaml_credentials: str,
                 **kwargs):
        super().__init__(*args, **kwargs)
        # Initialize all your variables here
        self.dag_name = dag_name
        self.is_retry = is_retry
        self.return_report = return_report
        self.enable_monitoring = enable_monitoring
        self.monitoring_dataset = monitoring_dataset
        self.monitoring_table = monitoring_table
        self.monitoring_bq_conn_id = monitoring_bq_conn_id
        bq_kwargs = {"bq_conn_id": bq_conn_id,
                     "bq_dataset_id": bq_dataset_id,
                     "bq_table_id": bq_table_id}
        ads_kwargs = {"api_version": api_version,
                      "google_ads_yaml_credentials": google_ads_yaml_credentials}
    

        self.input_hook = hook_factory.get_input_hook(hook_factory.InputHookType.BIG_QUERY, **bq_kwargs)
        self.output_hook = hook_factory.get_output_hook(hook_factory.OutputHookType.GOOGLE_ADS_OFFLINE_CONVERSIONS_V2, **ads_kwargs)
