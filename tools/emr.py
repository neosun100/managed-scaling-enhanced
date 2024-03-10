import boto3
from .utils import Utils  # 使用相对导入从同一包内导入Utils类


class AWSEMRClient:
    """
    一个专门用于与AWS EMR服务交互的类。
    """

    def __init__(self):
        self.emr_client = boto3.client('emr')

    @Utils.exception_handler
    def get_nodes_ec2_ids(self, cluster_id, instance_group_types_list=['TASK'], instance_states_list=['RUNNING']):
        """
        获取指定EMR集群所有Task Node的EC2 ID。
        :param cluster_id: EMR集群的ID
        :return: Task Node的EC2实例ID列表
        """
        Utils.logger.info(
            f"Fetching EC2 instance IDs for cluster '{cluster_id}'")

        instance_ids = []

        # 获取集群实例
        instances = self.emr_client.list_instances(
            ClusterId=cluster_id,
            InstanceGroupTypes=instance_group_types_list,
            InstanceStates=instance_states_list
        )

        # 提取EC2实例ID
        for instance in instances.get('Instances', []):
            instance_id = instance['Ec2InstanceId']
            instance_ids.append(instance_id)
            Utils.logger.info(f"Found instance ID: {instance_id}")

        Utils.logger.info(
            f"Found {len(instance_ids)} instance IDs for cluster '{cluster_id}'")

        return instance_ids
    

    @Utils.exception_handler
    def get_yarn_rm_url(self, cluster_id):
        """
        从指定的EMR集群获取YARN ResourceManager URL。

        :param emr_cluster_id: EMR集群ID
        :return: YARN ResourceManager URL
        """
        cluster_details = self.emr_client.describe_cluster(ClusterId=cluster_id)
        cluster_details = cluster_details['Cluster']

        # 获取主节点的公共DNS
        master_public_dns = cluster_details['MasterPublicDnsName']

        # 构造YARN ResourceManager URL
        yarn_rm_url = f'http://{master_public_dns}:8088'
        return yarn_rm_url

    @Utils.exception_handler
    def get_managed_scaling_policy(self, cluster_id):
        """
        获取指定EMR集群的Managed Scaling策略详情。
        :param cluster_id: EMR集群的ID
        :return: Managed Scaling策略详情
        """
        Utils.logger.info(
            f"Fetching Managed Scaling policy for cluster '{cluster_id}'")

        # 获取Managed Scaling策略
        policy = self.emr_client.get_managed_scaling_policy(
            ClusterId=cluster_id)

        Utils.logger.info(
            f"Managed Scaling policy for cluster '{cluster_id}': {policy}")

        return policy

    @Utils.exception_handler
    def put_managed_scaling_policy(self, cluster_id, policy):
        """
        修改指定EMR集群的Managed Scaling策略。
        :param cluster_id: EMR集群的ID
        :param policy: 新的Managed Scaling策略
        :return: 修改后的Managed Scaling策略详情
        """
        Utils.logger.info(
            f"Updating Managed Scaling policy for cluster '{cluster_id}'")

        # 修改Managed Scaling策略
        response = self.emr_client.put_managed_scaling_policy(
            ClusterId=cluster_id,
            ManagedScalingPolicy=policy
        )

        Utils.logger.info(
            f"Managed Scaling policy updated for cluster '{cluster_id}': {response}")

        return response


    @Utils.exception_handler
    def list_instance_fleets(self, ClusterId):
        """
        列出指定EMR集群的实例队列。

        :param ClusterId: EMR集群ID
        :return: 实例队列列表
        """
        response = self.emr_client.list_instance_fleets(ClusterId=ClusterId)
        Utils.logger.info(f"Instance fleets response: {response}")
        return response

    @Utils.exception_handler
    def modify_instance_fleet(self, ClusterId, InstanceFleet):
        """
        修改指定EMR集群的实例队列。

        :param ClusterId: EMR集群ID
        :param InstanceFleet: 实例队列配置
        :return: 修改后的实例队列配置
        """
        response = self.emr_client.modify_instance_fleet(
            ClusterId=ClusterId,
            InstanceFleet=InstanceFleet
        )
        return response