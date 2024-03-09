import asyncio
from tools.utils import Utils
from tools.cloudwatch import CloudWatchMetric
# 配置loguru的logger
Utils.logger.add("monitor_metrics.log",
                 format="{time} {level} {message}", level="DEBUG")

# 创建CloudWatchMetric实例
cw_metric = CloudWatchMetric(namespace='AWS/EC2')

# 定义你想查询的指标和维度
metric_name = 'CPUUtilization'
dimensions = [
    {
        'Name': 'InstanceId',
        'Value': 'i-010f7670bc2e3c620'
    },
    {
        'Name': 'InstanceId',
        'Value': 'i-086e6071fa9e570b1'
    },
    {
        'Name': 'InstanceId',
        'Value': 'i-0b067e216a89bc3f7'
    },
    {
        'Name': 'InstanceId',
        'Value': 'i-012c109d3201851c0'
    },
    {
        'Name': 'InstanceId',
        'Value': 'i-079e01e9ecf5ee6c3'
    },
]


response = asyncio.run(cw_metric.aioget_metric_statistics(metric_name, dimensions, minutes=100))
metricsList = [item['Average'] for item in response['Datapoints']]
Utils.logger.info(f"metricsList: {metricsList}")

