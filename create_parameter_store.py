import boto3
from functools import wraps
from loguru import logger
from utils import Utils
# 配置loguru的logger
logger.add("parameter_store.log",
           format="{time} {level} {message}", level="DEBUG")


@Utils.exception_handler
def write_parameters_to_parameter_store(parameters):
    """
    将参数写入AWS Parameter Store。

    :param parameters: 字典，包含参数名和值
    """
    ssm_client = boto3.client('ssm')

    for parameter, value in parameters.items():
        full_parameter_name = f"/{parameter}"
        try:
            response = ssm_client.put_parameter(
                Name=full_parameter_name,
                Value=str(value),
                Type='String',
                Overwrite=True
            )
            logger.info(
                f"Parameter '{full_parameter_name}' written: {response}")
        except boto3.exceptions.Boto3Error as e:  # 捕获boto3可能抛出的异常
            logger.error(
                f"Failed to write parameter '{full_parameter_name}': {e}")
            raise


def main():
    """
    主函数，用于定义参数并调用写入函数。
    """
    # 参数及其初始值
    # 定义前缀字符串变量
    prefix = "managedScalingEnhanced"

    # 使用prefix变量构建参数字典
    parameters = {
        f'{prefix}/minimumUnits': 128,
        f'{prefix}/maximumUnits': 512,
        f'{prefix}/spotInstancesTimeout': 300,
        f'{prefix}/monitorIntervalSeconds': 20,
        f'{prefix}/actionIntervalSeconds': 20,
        f'{prefix}/scaleOutAvgYARNMemoryAvailablePercentageValue': 75,
        f'{prefix}/scaleOutAvgYARNMemoryAvailablePercentageMinutes': 5,
        f'{prefix}/scaleOutAvgCapacityRemainingGBValue': 150,
        f'{prefix}/scaleOutAvgCapacityRemainingGBMinutes': 5,
        f'{prefix}/scaleOutAvgPendingAppNumValue': 10,
        f'{prefix}/scaleOutAvgPendingAppNumMinutes': 5,
        f'{prefix}/scaleOutAvgTaskNodeCPULoadValue': 80,
        f'{prefix}/scaleOutAvgTaskNodeCPULoadMinutes': 5,
        f'{prefix}/scaleInAvgYARNMemoryAvailablePercentageValue': 85,
        f'{prefix}/scaleInAvgYARNMemoryAvailablePercentageMinutes': 5,
        f'{prefix}/scaleInAvgCapacityRemainingGBValue': 200,
        f'{prefix}/scaleInAvgCapacityRemainingGBMinutes': 5,
        f'{prefix}/scaleInAvgPendingAppNumValue': 5,
        f'{prefix}/scaleInAvgPendingAppNumMinutes': 5,
        f'{prefix}/scaleInAvgTaskNodeCPULoadValue': 60,
        f'{prefix}/scaleInAvgTaskNodeCPULoadMinutes': 5,
    }

    write_parameters_to_parameter_store(parameters)


if __name__ == '__main__':
    main()
