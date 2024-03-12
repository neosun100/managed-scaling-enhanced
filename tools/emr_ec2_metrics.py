from .utils import Utils
from .cloudwatch import CloudWatchMetric
from .emr import AWSEMRClient
from collections import defaultdict

class NodeMetricsRetriever:
    def __init__(self, namespace='AWS/EC2', metric_name='CPUUtilization'):
        self.namespace = namespace
        self.metric_name = metric_name
        self.emr_client = AWSEMRClient()
        self.cw_metric = CloudWatchMetric(namespace=self.namespace)
        self.max_dimensions = 30  # 设置 Dimensions 参数的最大大小

    @Utils.exception_handler
    def get_task_node_metrics(self, emr_id, instance_group_types_list, instance_states_list, window_minutes):
        node_ids = self.emr_client.get_nodes_ec2_ids(
            emr_id, instance_group_types_list, instance_states_list)
        Utils.logger.info(f"{instance_group_types_list} {instance_states_list} window_minutes={window_minutes} nodeids: {node_ids}")

        if not node_ids:
            Utils.logger.warning(
                f"The current cluster has no task nodes in the {instance_group_types_list} {instance_states_list} window_minutes={window_minutes}.")
            return []

        metrics_list = []
        batches = [node_ids[i:i+self.max_dimensions] for i in range(0, len(node_ids), self.max_dimensions)]

        for batch in batches:
            dimensions = [{'Name': 'InstanceId', 'Value': id} for id in batch]
            response = self.cw_metric.get_metric_statistics(
                self.metric_name, dimensions, minutes=window_minutes)
            Utils.logger.info(f"{response}")

            batch_metrics = [metric['Average'] for metric in response['Datapoints']]
            metrics_list.append(batch_metrics)

        if metrics_list:
            # 对每个位置的指标值进行平均
            avg_metrics = []
            max_length = max(len(batch) for batch in metrics_list)
            for i in range(max_length):
                values = [batch[i] for batch in metrics_list if i < len(batch)]
                avg_metrics.append(sum(values) / len(values))

            Utils.logger.info(f"task node Average {self.metric_name} metric: {avg_metrics}")
            return avg_metrics
        else:
            Utils.logger.info(f"task node Average {self.metric_name} metricsList: []")
            return []