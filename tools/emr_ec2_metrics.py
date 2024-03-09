from .utils import Utils
from .cloudwatch import CloudWatchMetric
from .emr import AWSEMRClient


class NodeMetricsRetriever:
    def __init__(self, namespace='AWS/EC2', metric_name='CPUUtilization'):
        self.namespace = namespace
        self.metric_name = metric_name
        self.emr_client = AWSEMRClient()
        self.cw_metric = CloudWatchMetric(namespace=self.namespace)

    @Utils.exception_handler
    def get_task_node_metrics(self, emr_id, instance_group_types_list, instance_states_list, window_minutes):
        node_ids = self.emr_client.get_nodes_ec2_ids(
            emr_id, instance_group_types_list, instance_states_list)
        Utils.logger.info(f"{instance_group_types_list} {instance_states_list} window_minutes={window_minutes} nodeids: {node_ids}")

        if not node_ids:
            Utils.logger.info(
                f"The current cluster has no task nodes in the {instance_group_types_list} {instance_states_list} window_minutes={window_minutes}.")
            return []

        dimensions = [{'Name': 'InstanceId', 'Value': id}
                      for id in node_ids]
        Utils.logger.info(f"dimensions : {dimensions}")

        response = self.cw_metric.get_metric_statistics(
            self.metric_name, dimensions, minutes=window_minutes)
        Utils.logger.info(f"{response}")

        metrics_list = [metric['Average'] for metric in response['Datapoints']]
        Utils.logger.info(
            f"task node Average {self.metric_name} metricsList: {metrics_list}")
        return metrics_list
