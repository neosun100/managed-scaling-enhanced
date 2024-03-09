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

    @Utils.exception_handler
    def get_metric_statistics(self, metric, dimensions, minutes=15, statistics=None, period=60):
        """
        同步从CloudWatch获取指定指标的统计数据。
        """
        sync_client = boto3.client('cloudwatch')
        if statistics is None:
            statistics = ['Average']
        try:
            response = sync_client.get_metric_statistics(
                Namespace=self.namespace,
                MetricName=metric,
                Dimensions=dimensions,
                StartTime=datetime.utcnow() - timedelta(minutes=minutes),
                EndTime=datetime.utcnow(),
                Period=period,
                Statistics=statistics,
            )
            return response
        except Exception as e:
            Utils.logger.error(f"Failed to get metric statistics '{metric}': {e}")
            raise

    @Utils.exception_handler
    async def aioget_metric_statistics(self, metric, dimensions, minutes=15, statistics=None, period=60):
        """
        异步从CloudWatch获取指定指标的统计数据。
        """
        if statistics is None:
            statistics = ['Average']
        session = aioboto3.Session()
        async with session.client('cloudwatch') as async_client:
            try:
                response = await async_client.get_metric_statistics(
                    Namespace=self.namespace,
                    MetricName=metric,
                    Dimensions=dimensions,
                    StartTime=datetime.utcnow() - timedelta(minutes=minutes),
                    EndTime=datetime.utcnow(),
                    Period=period,
                    Statistics=statistics,
                )
                return response
            except Exception as e:
                Utils.logger.error(f"Failed to asynchronously get metric statistics '{metric}': {e}")
                raise