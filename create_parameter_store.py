from tools.utils import Utils
from tools.ssm import AWSSSMClient
# 配置loguru的logger
Utils.logger.add("parameter_store.log",
                 format="{time} {level} {message}", level="DEBUG")


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

    # 创建AWSSSMClient实例
    ssm_client = AWSSSMClient()
    # 调用write_parameters_to_parameter_store方法
    ssm_client.write_parameters_to_parameter_store(parameters)


if __name__ == '__main__':
    main()
