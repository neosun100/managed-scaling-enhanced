import argparse
import boto3
import time
from managed_scaling_enhanced import ManagedScalingEnhanced
from tools.utils import Utils
from apscheduler.schedulers.background import BackgroundScheduler

# 配置loguru的logger
Utils.logger.add("managed_scaling_enhanced.log", format="{time} {level} {message}", level="DEBUG")

def parse_arguments():
    parser = argparse.ArgumentParser(description='Managed Scaling Enhanced for EMR')
    parser.add_argument('--emr-id', required=True, help='EMR cluster ID')
    parser.add_argument('--prefix', default='managedScalingEnhanced', help='Parameter prefix (default: managedScalingEnhanced)')
    parser.add_argument('--spot-switch-on-demand', type=int, default=0, help='Whether to switch to on-demand instances (0: no, 1: yes, default: 0)')
    return parser.parse_args()

@Utils.exception_handler
def run_managed_scaling_enhanced(emr_id, prefix, spot_switch_on_demand):
    managed_scaling_enhanced_client = ManagedScalingEnhanced(emr_id=emr_id, prefix=prefix, spot_switch_on_demand=spot_switch_on_demand)

    scaleStatus = managed_scaling_enhanced_client.determine_scale_status()
    Utils.logger.info(f"Scale Status: {scaleStatus}")

    if scaleStatus == 1:
        Utils.logger.info("Executing scale out operation...")
        managed_scaling_enhanced_client.scale_out()
    elif scaleStatus == -1:
        Utils.logger.info("Executing scale in operation...")
        managed_scaling_enhanced_client.scale_in()
    else:
        Utils.logger.info("No scaling operation required.")

@Utils.exception_handler
def schedule_main(emr_id, prefix):
    """
    初始化调度器并添加run_managed_scaling_enhanced函数作为定时任务。

    :param emr_id: EMR集群ID
    :param prefix: 参数前缀
    """
    scheduler = BackgroundScheduler()

    # 从AWS参数存储中获取监控间隔时间
    ssm_client = boto3.client('ssm')
    monitor_interval_seconds = int(ssm_client.get_parameter(
        Name=f"/{prefix}/actionIntervalSeconds", WithDecryption=True)["Parameter"]["Value"])

    # 添加定时任务，每隔monitor_interval_seconds秒执行一次run_managed_scaling_enhanced函数
    scheduler.add_job(run_managed_scaling_enhanced, 'interval', args=[emr_id, prefix, args.spot_switch_on_demand], seconds=monitor_interval_seconds)

    # 启动调度器
    scheduler.start()

    try:
        # 主线程继续运行，直到按Ctrl+C或发生异常
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        # 关闭调度器
        scheduler.shutdown()
        Utils.logger.info("Scheduler shutdown successfully.")

if __name__ == '__main__':
    # 解析命令行参数
    args = parse_arguments()

    schedule_main(args.emr_id, args.prefix)

# python main.py --emr-id j-1F74M1P9SC57B --prefix managedScalingEnhanced 
    

    