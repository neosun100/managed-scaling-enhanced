import boto3
import aioboto3
import asyncio
from datetime import datetime, timedelta
from .utils import Utils  # 使用相对导入从同一包内导入Utils类


class CloudWatchMetric:
    """
    一个专门用于与AWS CloudWatch服务交互的类，提供同步和异步方法。
    """

    def __init__(self, namespace):
        self.namespace = namespace
        Utils.logger.info(
            f"CloudWatchMetric instance created with namespace: {namespace}")

    @Utils.exception_handler
    def get_metric_statistics(self, metric, dimensions, minutes=15, statistics=None, period=60):
        """
        同步从CloudWatch获取指定指标的统计数据。
        """
        Utils.logger.info(
            f"Retrieving metric statistics for '{metric}' (sync)")
        sync_client = boto3.client('cloudwatch')
        if statistics is None:
            statistics = ['Average']
        response = sync_client.get_metric_statistics(
            Namespace=self.namespace,
            MetricName=metric,
            Dimensions=dimensions,
            StartTime=datetime.utcnow() - timedelta(minutes=minutes),
            EndTime=datetime.utcnow(),
            Period=period,
            Statistics=statistics,
        )
        Utils.logger.info(
            f"Successfully retrieved metric statistics for '{metric}' (sync)")
        return response

    @Utils.exception_handler
    async def aioget_metric_statistics(self, metric, dimensions, minutes=15, statistics=None, period=60):
        """
        异步从CloudWatch获取指定指标的统计数据。
        """
        Utils.logger.info(
            f"Retrieving metric statistics for '{metric}' (async)")
        if statistics is None:
            statistics = ['Average']
        session = aioboto3.Session()
        async with session.client('cloudwatch') as async_client:
            response = await async_client.get_metric_statistics(
                Namespace=self.namespace,
                MetricName=metric,
                Dimensions=dimensions,
                StartTime=datetime.utcnow() - timedelta(minutes=minutes),
                EndTime=datetime.utcnow(),
                Period=period,
                Statistics=statistics,
            )
            Utils.logger.info(
                f"Successfully retrieved metric statistics for '{metric}' (async)")
            return response
