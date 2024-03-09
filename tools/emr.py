import boto3
from .utils import Utils  # 使用相对导入从同一包内导入Utils类

class AWSEMRClient:
    """
    一个专门用于与AWS EMR服务交互的类。
    """

    def __init__(self):
        self.emr_client = boto3.client('emr')

    @Utils.exception_handler
    def get_nodes_ec2_ids(self, cluster_id, instance_group_types_list=['TASK'],instance_states_list=['RUNNING']):
        """
        获取指定EMR集群所有Task Node的EC2 ID。
        :param cluster_id: EMR集群的ID
        :return: Task Node的EC2实例ID列表
        """
        instance_ids = []
        try:
            # 获取集群实例
            instances = self.emr_client.list_instances(
                ClusterId=cluster_id,
                InstanceGroupTypes=instance_group_types_list,
                InstanceStates=instance_states_list
            )
            # 提取EC2实例ID
            for instance in instances.get('Instances', []):
                instance_ids.append(instance['Ec2InstanceId'])
        except Exception as e:
            Utils.logger.error(f"Failed to get task node EC2 IDs for cluster '{cluster_id}': {e}")
            raise
        return instance_ids
