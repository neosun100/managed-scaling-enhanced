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
        f'{prefix}/minimumUnits': 320,  # max nuit 的最小值
        f'{prefix}/maximumUnits': 1000, # max nuit 的最大值

        f'{prefix}/spotInstancesTimeout': 60*30,
        f'{prefix}/monitorIntervalSeconds': 30,
        f'{prefix}/actionIntervalSeconds': 30,

        f'{prefix}/scaleOutAvgYARNMemoryAvailablePercentageValue': 70,
        f'{prefix}/scaleOutAvgYARNMemoryAvailablePercentageMinutes': 5,
        f'{prefix}/scaleOutAvgCapacityRemainingGBValue': 256,
        f'{prefix}/scaleOutAvgCapacityRemainingGBMinutes': 5,
        f'{prefix}/scaleOutAvgPendingAppNumValue': 3,
        f'{prefix}/scaleOutAvgPendingAppNumMinutes': 5,
        f'{prefix}/scaleOutAvgTaskNodeCPULoadValue': 20,
        f'{prefix}/scaleOutAvgTaskNodeCPULoadMinutes': 15,

        f'{prefix}/scaleInAvgYARNMemoryAvailablePercentageValue': 30,
        f'{prefix}/scaleInAvgYARNMemoryAvailablePercentageMinutes': 3,
        f'{prefix}/scaleInAvgCapacityRemainingGBValue': 512,
        f'{prefix}/scaleInAvgCapacityRemainingGBMinutes': 3,
        f'{prefix}/scaleInAvgPendingAppNumValue': 2,
        f'{prefix}/scaleInAvgPendingAppNumMinutes': 2,
        f'{prefix}/scaleInAvgTaskNodeCPULoadValue': 40,
        f'{prefix}/scaleInAvgTaskNodeCPULoadMinutes': 15,

        f'{prefix}/scaleOutFactor': 1.5,
        f'{prefix}/scaleInFactor': 1.7,   

        f'{prefix}/maximumOnDemandInstancesNumValue': 160,  # 针对defalut情况下：on_demand instance num = core node num         
    }

    # 创建AWSSSMClient实例
    ssm_client = AWSSSMClient()
    # 调用write_parameters_to_parameter_store方法
    ssm_client.write_parameters_to_parameter_store(parameters)


if __name__ == '__main__':
    main()
