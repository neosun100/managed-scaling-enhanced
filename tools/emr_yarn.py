import sqlite3
import time
import boto3
from .utils import Utils
from .emr import AWSEMRClient
import requests

class EMRMetricManager:
    """
    一个专门用于管理EMR集群指标的类。
    """

    def __init__(self):
        self.ssm_client = boto3.client('ssm')

    @Utils.exception_handler
    def sanitize_table_name(self, table_name):
        """
        替换表名中的非法字符。

        :param table_name: 原始表名
        :return: 经过处理的表名
        """
        return ''.join(c if c.isalnum() or c == '_' else '_' for c in table_name)

    @Utils.exception_handler
    def get_data_from_sqlite(self, emr_cluster_id='j-1F74M1P9SC57B', metric_name='PendingAppNum', prefix='managedScalingEnhanced', ScaleStatus='scaleOut'):
        """
        从SQLite数据库中获取指定指标的数据。

        :param emr_cluster_id: EMR集群ID
        :param metric_name: 需要查询的指标名称
        :param prefix: 参数前缀
        :param ScaleStatus: 扩缩容状态 ('scaleOut' 或 'scaleIn')
        :return: 指定时间范围内的指标数据列表
        """
        # 1. 通过emr_cluster_id，查询对应的sqlite文件和表名
        table_name = self.sanitize_table_name(emr_cluster_id.replace('-', '_'))

        # 2. 检查metric_name是否合法
        valid_metrics = ['PendingAppNum', 'CapacityRemainingGB', 'YARNMemoryAvailablePercentage']
        if metric_name not in valid_metrics:
            Utils.logger.error(f"Invalid metric name: {metric_name}. Valid metrics are: {', '.join(valid_metrics)}")
            return []

        # 3. 通过prefix，得到monitor_interval_seconds
        monitor_interval_seconds = int(self.ssm_client.get_parameter(
            Name=f"/{prefix}/monitorIntervalSeconds", WithDecryption=True)["Parameter"]["Value"])

        # 4. 根据ScaleStatus和metric_name获取时间窗口
        if ScaleStatus == 'scaleOut':
            time_range_param_name = f"/{prefix}/scaleOutAvg{metric_name}Minutes"
        elif ScaleStatus == 'scaleIn':
            time_range_param_name = f"/{prefix}/scaleInAvg{metric_name}Minutes"
        else:
            Utils.logger.error(f"Invalid ScaleStatus: {ScaleStatus}. ScaleStatus should be 'scaleOut' or 'scaleIn'.")
            return []

        time_range_minutes = int(self.ssm_client.get_parameter(
            Name=time_range_param_name, WithDecryption=True)["Parameter"]["Value"])

        # 计算时间范围
        end_time = int(time.time())
        start_time = end_time - time_range_minutes * 60

        # 连接到SQLite数据库
        conn = sqlite3.connect(f"{table_name}.db")
        cursor = conn.cursor()

        # 查询指定时间范围内的数据
        cursor.execute(f"SELECT {metric_name} FROM {table_name} WHERE Timestamp BETWEEN {start_time} AND {end_time} ORDER BY Timestamp")
        records = [row[0] for row in cursor.fetchall()]

        # 计算在时间范围内应该有多少个数据点
        expected_data_points = time_range_minutes * 60 // monitor_interval_seconds

        # 检查数据是否足够
        if len(records) < expected_data_points * 0.8:
            Utils.logger.warning(
                f"Not enough data in the specified time range ({time_range_minutes} minutes) for metric '{metric_name}'. Expected {expected_data_points} data points, but only got {len(records)}.")
            return []

        # 关闭数据库连接
        conn.close()

        return records


    @Utils.exception_handler
    def get_yarn_metrics(self, emr_cluster_id='j-1F74M1P9SC57B',metrics_name='pendingVirtualCores'):
        """
        从 YARN ResourceManager 获取指定指标的数据。

        :param emr_cluster_id: EMR 集群 ID
        :param metrics_name: 需要查询的当前yarn metrics_name ： 目前关注如下：pendingVirtualCores，appsPending，totalVirtualCores
        :return: 包含 pendingVirtualCores 和 appsPending 指标的字典
        """

        emr_client = AWSEMRClient()
        yarn_rm_url = emr_client.get_yarn_rm_url(emr_cluster_id)

        # 获取 pendingVirtualCores 指标
        response = requests.get(f"{yarn_rm_url}/ws/v1/cluster/metrics")
        response.raise_for_status()
        metrics = response.json().get('clusterMetrics', {})
        return metrics.get(metrics_name, 0)

