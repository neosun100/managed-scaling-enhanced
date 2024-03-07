from apscheduler.schedulers.background import BackgroundScheduler
import requests
import boto3
import time
from functools import wraps
from loguru import logger
from boto3.dynamodb.conditions import Key
import pandas as pd

# 配置loguru日志
logger.add("debug.log", format="{time} {level} {message}", level="DEBUG")

# YARN ResourceManager URL
yarn_rm_url = 'http://10.68.1.77:8088'

# AWS DynamoDB 配置
dynamodb = boto3.resource('dynamodb')
table_name = 'EMR-YARN-Metric-table'  # 替换为您的DynamoDB表名


def exception_handler(func):
    """
    异常处理装饰器，用于捕获和记录函数中的异常。

    :param func: 被装饰的函数
    :return: 装饰器包装后的函数
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"An error occurred in {func.__name__}: {e}")
            raise  # 重新抛出异常以便外部捕获
    return wrapper


@exception_handler
def check_or_create_table(dynamodb_resource, table_name):
    """
    检查DynamoDB表是否存在，如果不存在，则创建。

    :param dynamodb_resource: boto3 DynamoDB资源
    :param table_name: DynamoDB表名
    """
    try:
        table = dynamodb_resource.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'Timestamp',
                    'KeyType': 'HASH'  # 分区键
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'Timestamp',
                    'AttributeType': 'N'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1
            }
        )
        table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
        logger.info(f"Table {table_name} created successfully.")
    except dynamodb_resource.meta.client.exceptions.ResourceInUseException:
        logger.info(f"Table {table_name} already exists.")


@exception_handler
def get_cluster_metrics(yarn_rm_url):
    """
    从YARN ResourceManager获取集群指标。

    :param yarn_rm_url: YARN ResourceManager的URL
    :return: YARN集群指标的字典
    """
    response = requests.get(f'{yarn_rm_url}/ws/v1/cluster/metrics')
    response.raise_for_status()  # 如果响应状态码不是200，将抛出HTTPError异常
    metrics_data = response.json()
    return metrics_data.get('clusterMetrics', {})


@exception_handler
def write_metrics_to_dynamodb(table, metrics):
    """
    将指标数据写入DynamoDB表。

    :param table: DynamoDB表的引用
    :param metrics: 需要写入的指标数据
    """
    ttl = int(time.time()) + 30 * 24 * 60 * 60  # 计算TTL值（30天后）
    data_to_write = {"Timestamp": int(time.time()), "TTL": ttl}
    data_to_write.update(metrics)
    table.put_item(Item=data_to_write)
    logger.info("Data written to DynamoDB successfully.")


@exception_handler
def print_latest_record(dynamodb_resource, table_name):
    """
    打印DynamoDB表的最新一条记录。
    注意：这使用了Scan操作，适用于小表。大型表应避免使用Scan。

    :param dynamodb_resource: boto3 DynamoDB资源
    :param table_name: DynamoDB表名
    """
    table = dynamodb_resource.Table(table_name)
    # 使用 Scan 操作获取所有记录，然后找到最新的一条记录
    response = table.scan()
    items = response.get('Items', [])
    if items:
        # 假设 Timestamp 是数字类型，并找到最大值
        latest_record = max(items, key=lambda x: x['Timestamp'])
        logger.info(f"Latest record in DynamoDB: {latest_record}")
    else:
        logger.info(f"No records found in DynamoDB table {table_name}.")


@exception_handler
def print_latest_record_as_dataframe(dynamodb_resource, table_name):
    """
    直接打印DynamoDB表的最新一条记录，以Pandas DataFrame的形式。
    注意：这使用了Scan操作，适用于小表。大型表应避免使用Scan。

    :param dynamodb_resource: boto3 DynamoDB资源
    :param table_name: DynamoDB表名
    """
    table = dynamodb_resource.Table(table_name)
    # 使用 Scan 操作获取所有记录，然后找到最新的一条记录
    response = table.scan()
    items = response.get('Items', [])
    if items:
        # 假设 Timestamp 是数字类型，并找到最大值
        latest_record = max(items, key=lambda x: x['Timestamp'])
        # 将记录转换为Pandas DataFrame
        df = pd.DataFrame([latest_record])
        print("Latest record in DynamoDB:")
        print(df.to_string(index=False))  # 直接打印DataFrame，不显示索引
    else:
        print(f"No records found in DynamoDB table {table_name}.")


@exception_handler
def print_latest_total_virtual_cores(dynamodb_resource, table_name):
    """
    打印DynamoDB表中最新一条记录的availableVirtualCores值。
    注意：这使用了Scan操作，适用于小表。大型表应避免使用Scan。

    :param dynamodb_resource: boto3 DynamoDB资源
    :param table_name: DynamoDB表名
    """
    table = dynamodb_resource.Table(table_name)
    # 使用 Scan 操作获取所有记录，然后找到最新的一条记录
    response = table.scan()
    items = response.get('Items', [])
    if items:
        # 假设 Timestamp 是数字类型，并找到最大值
        latest_record = max(items, key=lambda x: x['Timestamp'])
        # 获取最新记录的 availableVirtualCores 值
        latest_total_virtual_cores = latest_record.get('totalVirtualCores')
        print("Latest totalVirtualCores value in DynamoDB:")
        print(latest_total_virtual_cores)
    else:
        print(f"No records found in DynamoDB table {table_name}.")


def metric_table_main():
    """
    包含了所有的metric_table操作步骤。
    """

    # 检查表是否存在，如果不存在，则创建
    check_or_create_table(dynamodb, table_name)

    # 获取集群指标
    metrics = get_cluster_metrics(yarn_rm_url)

    # 获取DynamoDB表的引用
    table = dynamodb.Table(table_name)

    # 写入指标到DynamoDB
    write_metrics_to_dynamodb(table, metrics)

    # 打印最新记录
    print_latest_record(dynamodb, table_name)

    # 打印最新记录的availableVirtualCores值
    print_latest_total_virtual_cores(dynamodb, table_name)


def schedule_main():
    """
    初始化调度器并添加main函数作为定时任务。
    """
    scheduler = BackgroundScheduler()

    # 采集数据间隔
    ssm = boto3.client("ssm")
    monitor_interval_seconds = int(ssm.get_parameter(
        Name="/fleet-settings/monitor-interval-seconds", WithDecryption=True)["Parameter"]["Value"])

    # 添加定时任务，每隔5秒执行一次main函数
    scheduler.add_job(metric_table_main, 'interval',
                      seconds=monitor_interval_seconds)

    # 启动调度器
    scheduler.start()

    try:
        # 主线程继续运行，直到按Ctrl+C或发生异常
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        # 关闭调度器
        scheduler.shutdown()
        logger.info("Scheduler shutdown successfully.")


if __name__ == '__main__':
    schedule_main()
