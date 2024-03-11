from apscheduler.schedulers.background import BackgroundScheduler
import requests
import sqlite3
import time
from functools import wraps
from loguru import logger
import json
import boto3
import argparse
from datetime import datetime, timedelta
import random

# 配置loguru日志
logger.add("debug.log", format="{time} {level} {message}", level="DEBUG")

# AWS EMR 配置
emr_client = boto3.client('emr')


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
def get_yarn_rm_url(emr_cluster_id):
    """
    从指定的EMR集群获取YARN ResourceManager URL。

    :param emr_cluster_id: EMR集群ID
    :return: YARN ResourceManager URL
    """
    cluster_details = emr_client.describe_cluster(ClusterId=emr_cluster_id)
    cluster_details = cluster_details['Cluster']

    # 获取主节点的公共DNS
    master_public_dns = cluster_details['MasterPublicDnsName']

    # 构造YARN ResourceManager URL
    yarn_rm_url = f'http://{master_public_dns}:8088'
    return yarn_rm_url


@exception_handler
def get_random_yarn_rm_url(emr_cluster_id):
    """
    从指定的EMR集群获取一个随机的YARN ResourceManager URL。

    :param emr_cluster_id: EMR集群ID
    :return: 一个随机的YARN ResourceManager URL
    """
    cluster_details = emr_client.describe_cluster(ClusterId=emr_cluster_id)
    cluster_details = cluster_details['Cluster']

    # 获取所有主节点的公共DNS
    if 'MasterPublicDnsNameList' in cluster_details:
        # 多主节点架构
        master_public_dns_list = [instance['PublicDnsName'] for instance in cluster_details['MasterPublicDnsNameList']]
    else:
        # 单主节点架构
        master_public_dns_list = [cluster_details['MasterPublicDnsName']]

    # 构造所有YARN ResourceManager URLs
    yarn_rm_urls = [f'http://{master_public_dns}:8088' for master_public_dns in master_public_dns_list]

    # 随机选择一个URL
    random_yarn_rm_url = random.choice(yarn_rm_urls)
    return random_yarn_rm_url

@exception_handler
def get_yarn_rm_urls(emr_cluster_id):
    """
    从指定的EMR集群获取所有YARN ResourceManager URLs。

    :param emr_cluster_id: EMR集群ID
    :return: 所有YARN ResourceManager URLs的列表
    """
    cluster_details = emr_client.describe_cluster(ClusterId=emr_cluster_id)
    cluster_details = cluster_details['Cluster']

    # 获取所有主节点的公共DNS
    master_public_dns_list = [instance['PublicDnsName'] for instance in cluster_details['MasterPublicDnsNameList']]

    # 构造所有YARN ResourceManager URLs
    yarn_rm_urls = [f'http://{master_public_dns}:8088' for master_public_dns in master_public_dns_list]
    return yarn_rm_urls


@exception_handler
def sanitize_table_name(table_name):
    """
    替换表名中的非法字符。

    :param table_name: 原始表名
    :return: 经过处理的表名
    """
    return ''.join(c if c.isalnum() or c == '_' else '_' for c in table_name)


@exception_handler
def create_table(conn, table_name):
    """
    创建SQLite表，如果表不存在。

    :param conn: SQLite数据库连接
    :param table_name: SQLite表名
    """
    sanitized_table_name = sanitize_table_name(table_name)

    cursor = conn.cursor()
    cursor.execute(f'''CREATE TABLE IF NOT EXISTS {sanitized_table_name}
                      (Timestamp INTEGER PRIMARY KEY, PendingAppNum INTEGER, CapacityRemainingGB REAL, YARNMemoryAvailablePercentage REAL)''')
    conn.commit()
    logger.info(f"Table '{sanitized_table_name}' created successfully.")


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
def write_metrics_to_sqlite(conn, table_name, metrics):
    """
    将指标数据写入SQLite表。

    :param conn: SQLite数据库连接
    :param table_name: SQLite表名
    :param metrics: 需要写入的指标数据
    """
    sanitized_table_name = sanitize_table_name(table_name)

    cursor = conn.cursor()
    timestamp = int(time.time())
    pending_app_num = metrics.get('appsPending', 0)
    available_mb = metrics.get('availableMB', 0)
    reserved_mb = metrics.get('reservedMB', 0)
    allocated_mb = metrics.get('allocatedMB', 0)
    capacity_remaining_gb = available_mb / 1024
    yarn_memory_available_percentage = available_mb / \
        (available_mb + reserved_mb + allocated_mb) if (available_mb +
                                                        reserved_mb + allocated_mb) != 0 else 0

    cursor.execute(f"INSERT INTO {sanitized_table_name} (Timestamp, PendingAppNum, CapacityRemainingGB, YARNMemoryAvailablePercentage) VALUES (?, ?, ?, ?)",
                   (timestamp, pending_app_num, capacity_remaining_gb, yarn_memory_available_percentage))
    conn.commit()
    logger.info("Data written to SQLite successfully.")

    # 打印最新记录
    cursor.execute(
        f"SELECT * FROM {sanitized_table_name} ORDER BY Timestamp DESC LIMIT 1")
    latest_record = cursor.fetchone()
    if latest_record:
        logger.info(
            f"Latest record in SQLite table '{sanitized_table_name}': {latest_record}")
    else:
        logger.info(
            f"No records found in SQLite table '{sanitized_table_name}'.")

    # 删除早于当前时间30天的记录
    thirty_days_ago = int(time.time()) - 30 * 24 * 60 * 60
    cursor.execute(
        f"DELETE FROM {sanitized_table_name} WHERE Timestamp < {thirty_days_ago}")
    conn.commit()
    logger.info(
        f"Records older than 30 days have been deleted from SQLite table '{sanitized_table_name}'.")


def metric_table_main(emr_cluster_id, table_name):
    """
    包含了所有的metric_table操作步骤。

    :param emr_cluster_id: EMR集群ID
    :param table_name: SQLite表名
    """

    # 连接到SQLite数据库
    conn = sqlite3.connect(f"{table_name}.db")

    # 创建表（如果不存在）
    create_table(conn, table_name)

    # 获取YARN ResourceManager URL
    yarn_rm_url = get_random_yarn_rm_url(emr_cluster_id)

    # 获取集群指标
    metrics = get_cluster_metrics(yarn_rm_url)

    # 写入指标到SQLite
    write_metrics_to_sqlite(conn, table_name, metrics)

    # 关闭数据库连接
    conn.close()


def schedule_main(emr_cluster_id, prefix):
    """
    初始化调度器并添加main函数作为定时任务。

    :param emr_cluster_id: EMR集群ID
    :param prefix: 参数前缀
    """
    scheduler = BackgroundScheduler()

    # 从AWS参数存储中获取监控间隔时间
    ssm_client = boto3.client('ssm')
    monitor_interval_seconds = int(ssm_client.get_parameter(
        Name=f"/{prefix}/monitorIntervalSeconds", WithDecryption=True)["Parameter"]["Value"])

    # 构造SQLite表名
    table_name = sanitize_table_name(emr_cluster_id.replace('-', '_'))

    # 添加定时任务，每隔monitor_interval_seconds秒执行一次main函数
    scheduler.add_job(metric_table_main, 'interval', args=[
                      emr_cluster_id, table_name], seconds=monitor_interval_seconds)

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
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='EMR YARN Metric Monitor')
    parser.add_argument('--emr-cluster-id', required=True,
                        help='EMR cluster ID')
    parser.add_argument('--prefix', required=True,
                        help='Parameter store prefix')
    args = parser.parse_args()

    schedule_main(args.emr_cluster_id, args.prefix)
