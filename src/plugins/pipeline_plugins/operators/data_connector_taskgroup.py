from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from pipeline_plugins.utils import hook_factory

from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

from pipeline_plugins.hooks import monitoring_hook as monitoring
from pipeline_plugins.utils import errors


def data_connector_task_group(
        data_product_tag: str,
        fetch_data_args: dict = {},
        send_events_args: dict = {},
        monitor_args: dict = {},
        group_id="data_connector_task_group",
) -> TaskGroup:
    """
    Contains tasks for fetching data, sending events, and monitoring.

    Args:
        data_product_tag (str): A unique identifier for this pipeline, used to simplify monitoring and auditing.
        fetch_data_args (dict): Arguments to pass to the FetchDataTask.
        send_events_args (dict): Arguments to pass to the SendEventsTask.
        monitor_args (dict): Arguments to pass to the MonitorTask.
        group_id (str): The ID for the `TaskGroup`.

    Returns:
        A `TaskGroup` containing the tasks for fetching data, sending events, and monitoring.
    """

    with TaskGroup(group_id=group_id) as tasks:
        fetch_data = FetchDataTask(task_id="fetch_data", **fetch_data_args)
        send_events = SendEventsTask(task_id="send_events", **send_events_args)
        monitor = MonitorTask(task_id="monitor", **monitor_args)

        update_telemetry = DummyOperator(task_id="update_telemetry")
        audit_event = DummyOperator(task_id="audit_event")

        fetch_data >> send_events >> monitor >> audit_event >> update_telemetry

    return tasks


class FetchDataTask(BaseOperator):
    @apply_defaults
    def __init__(self,
                 input_hook: hook_factory.InputHookType,
                 is_retry: bool = False,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.input_hook = hook_factory.get_input_hook(input_hook, **kwargs)
        self.is_retry = is_retry

    def execute(self, context):
        if self.is_retry:
            blob_generator = self.monitor.events_blobs_generator()
        else:
            processed_blobs_generator = self.monitor.generate_processed_blobs_ranges()
            blob_generator = self.input_hook.events_blobs_generator(
                processed_blobs_generator=processed_blobs_generator)
            
        blobs = []
        for blb in blob_generator:
            if blb:
                blobs.append(blb)
                
        return blobs


class SendEventsTask(BaseOperator):
    @apply_defaults
    def __init__(self,
                 output_hook: hook_factory.OutputHookType,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output_hook = hook_factory.get_output_hook(output_hook, **kwargs)

    def execute(self, context):
        # The input to this task should be the output from FetchDataTask
        blobs = context['task_instance'].xcom_pull(task_ids='fetch_data')

        reports = []
        for blb in blobs:
            if blb:
                blb = self.output_hook.send_events(blb)
                reports.append(blb.reports)

        return reports



class MonitorTask(BaseOperator):
    @apply_defaults
    def __init__(self,
                 dag_name: str,
                 monitoring_dataset: str = '',
                 monitoring_table: str = '',
                 monitoring_bq_conn_id: str = '',
                 enable_monitoring: bool = True,
                 input_hook: hook_factory.InputHookType,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dag_name = dag_name
        self.enable_monitoring = enable_monitoring

        if enable_monitoring and not all([monitoring_dataset,
                                          monitoring_table,
                                          monitoring_bq_conn_id]):
            raise errors.MonitoringValueError(
                msg=('Missing or empty monitoring parameters although monitoring is '
                     'enabled.'),
                error_num=errors.ErrorNameIDMap.MONITORING_HOOK_INVALID_VARIABLES)

        self.monitor = monitoring.MonitoringHook(
            bq_conn_id=monitoring_bq_conn_id,
            enable_monitoring=enable_monitoring,
            dag_name=dag_name,
            monitoring_dataset=monitoring_dataset,
            monitoring_table=monitoring_table,
            location=hook_factory.get_input_hook(input_hook, **kwargs).get_location())

    def execute(self, context):
        # The input to this task should be the output from FetchDataTask
        blobs = context['task_instance'].xcom_pull(task_ids='fetch_data')

        if self.enable_monitoring:
            for blb in blobs:
                if blb:
                    self.monitor.store_blob(dag_name=self.dag_name,
                                            location=blb.location,
                                            position=blb.position,
                                            num_rows=blb.num_rows)
                    self.monitor.store_events(dag_name=self.dag_name,
                                              location=blb.location,
                                              id_event_error_tuple_list=blb.failed_events)
