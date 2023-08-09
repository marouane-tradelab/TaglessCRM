from typing import Optional


from airflow.models import BaseOperator
from pipeline_plugins.hooks import monitoring_hook as monitoring
from pipeline_plugins.utils import hook_factory


class BaseDataConnectorOperator(BaseOperator):

    def execute(self, context):
        # Create your input and output hooks
        # Create your monitoring hook
        monitor = monitoring.MonitoringHook(
            bq_conn_id=self.monitoring_bq_conn_id,
            enable_monitoring=self.enable_monitoring,
            dag_name=self.dag_name,
            monitoring_dataset=self.monitoring_dataset,
            monitoring_table=self.monitoring_table,
            location=self.input_hook.get_location())
        
        if self.is_retry:
            blob_generator = monitor.events_blobs_generator()
        else:
            processed_blobs_generator = monitor.generate_processed_blobs_ranges()
            blob_generator = self.input_hook.events_blobs_generator(
                processed_blobs_generator=processed_blobs_generator)

        reports = []
        for blb in blob_generator:
            if blb:
                blb = self.output_hook.send_events(blb)
                reports.append(blb.reports)

                if self.enable_monitoring:
                    monitor.store_blob(dag_name=self.dag_name,
                                       location=blb.location,
                                       position=blb.position,
                                       num_rows=blb.num_rows)
                    monitor.store_events(dag_name=self.dag_name,
                                         location=blb.location,
                                         id_event_error_tuple_list=blb.failed_events)

        if self.return_report:
            return reports

class BigQueryToAdsCMOperatorV2(BaseDataConnectorOperator):
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
                 ads_upload_key_type: str,
                 ads_cm_app_id: Optional[str],
                 ads_cm_create_list: bool,
                 ads_cm_membership_lifespan_in_days: int,
                 ads_cm_user_list_name: str,
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
        ads_kwargs = {
            "api_version": api_version,
            "google_ads_yaml_credentials": google_ads_yaml_credentials,
            "ads_upload_key_type": ads_upload_key_type,
            "ads_cm_app_id": ads_cm_app_id,
            "ads_cm_create_list": ads_cm_create_list,
            "ads_cm_membership_lifespan_in_days": ads_cm_membership_lifespan_in_days,
            "ads_cm_user_list_name": ads_cm_user_list_name,
        }

        self.input_hook = hook_factory.get_input_hook(hook_factory.InputHookType.BIG_QUERY, **bq_kwargs)
        self.output_hook = hook_factory.get_output_hook(hook_factory.OutputHookType.GOOGLE_ADS_CUSTOMER_MATCH_V2, **ads_kwargs)



class BigQueryToAdsOCOperator(BaseDataConnectorOperator):
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

