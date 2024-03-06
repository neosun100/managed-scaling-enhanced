import boto3
from loguru import logger
from functools import wraps
import statistics
from utils import Utils

# 配置loguru的logger
logger.add("managed_scaling_enhanced.log", format="{time} {level} {message}", level="DEBUG")

# 初始化Boto3 SSM客户端
ssm_client = boto3.client('ssm')


@Utils.exception_handler
def get_parameter(name):
    """
    从AWS Parameter Store获取参数值。
    :param name: 参数的名称
    :return: 参数的值
    """
    try:
        parameter = ssm_client.get_parameter(Name=name, WithDecryption=True)
        return parameter['Parameter']['Value']
    except Exception as e:
        logger.error(f"Failed to get parameter '{name}': {e}")
        raise

@Utils.exception_handler
def determine_scale_status(YARNMemoryAvailablePercentageList, CapacityRemainingGBList, pendingAppNumList, taskNodeCPULoadList, currentMaxUnitNum):
    """
    根据输入的监控数据和当前最大单元数，决定是否扩缩容。
    """
    # 定义前缀字符串变量
    prefix = "managedScalingEnhanced"

    # 从Parameter Store获取阈值参数
    minimumUnits = int(Utils.get_parameter(f'{prefix}/minimumUnits'))
    maximumUnits = int(Utils.get_parameter(f'{prefix}/maximumUnits'))
    scaleOutAvgYARNMemoryAvailablePercentageValue = float(Utils.get_parameter(f'{prefix}/scaleOutAvgYARNMemoryAvailablePercentageValue'))
    scaleOutAvgCapacityRemainingGBValue = float(Utils.get_parameter(f'{prefix}/scaleOutAvgCapacityRemainingGBValue'))
    scaleOutAvgPendingAppNumValue = float(Utils.get_parameter(f'{prefix}/scaleOutAvgPendingAppNumValue'))
    scaleOutAvgTaskNodeCPULoadValue = float(Utils.get_parameter(f'{prefix}/scaleOutAvgTaskNodeCPULoadValue'))
    scaleInAvgYARNMemoryAvailablePercentageValue = float(Utils.get_parameter(f'{prefix}/scaleInAvgYARNMemoryAvailablePercentageValue'))
    scaleInAvgCapacityRemainingGBValue = float(Utils.get_parameter(f'{prefix}/scaleInAvgCapacityRemainingGBValue'))
    scaleInAvgPendingAppNumValue = float(Utils.get_parameter(f'{prefix}/scaleInAvgPendingAppNumValue'))
    scaleInAvgTaskNodeCPULoadValue = float(Utils.get_parameter(f'{prefix}/scaleInAvgTaskNodeCPULoadValue'))

    # 判断逻辑
    # scaleOut单项条件状态
    scaleOutMemoryConditionYARNMemoryAvailablePercentageStatus = statistics.mean(YARNMemoryAvailablePercentageList) <= scaleOutAvgYARNMemoryAvailablePercentageValue
    scaleOutMemoryConditionCapacityRemainingGBStatus = statistics.mean(CapacityRemainingGBList) <= scaleOutAvgCapacityRemainingGBValue
    scaleOutAppConditionPendingAppNumStatus = statistics.mean(pendingAppNumList) >= scaleOutAvgPendingAppNumValue
    scaleOutCPULoadStatus = statistics.mean(taskNodeCPULoadList) >= scaleOutAvgTaskNodeCPULoadValue
    scaleOutcurrentMaxUnitNumStatus = currentMaxUnitNum < maximumUnits

    # scaleOut综合条件
    scaleOutMemoryCondition = scaleOutMemoryConditionYARNMemoryAvailablePercentageStatus or scaleOutMemoryConditionCapacityRemainingGBStatus
    scaleOutPendingAppNumCondition = scaleOutAppConditionPendingAppNumStatus
    scaleOutCPULoadCondition = scaleOutCPULoadStatus
    scaleOutcurrentMaxUnitCondition = scaleOutcurrentMaxUnitNumStatus
    scaleOutCondition = scaleOutMemoryCondition and scaleOutPendingAppNumCondition and scaleOutCPULoadCondition and scaleOutcurrentMaxUnitCondition

    # scaleIn单项条件状态
    scaleInMemoryConditionYARNMemoryAvailablePercentageStatus = statistics.mean(YARNMemoryAvailablePercentageList) > scaleInAvgYARNMemoryAvailablePercentageValue
    scaleInMemoryConditionCapacityRemainingGBStatus = statistics.mean(CapacityRemainingGBList) > scaleInAvgCapacityRemainingGBValue
    scaleInAppConditionPendingAppNumStatus = statistics.mean(pendingAppNumList) < scaleInAvgPendingAppNumValue
    scaleInCPULoadStatus = statistics.mean(taskNodeCPULoadList) < scaleInAvgTaskNodeCPULoadValue
    scaleIncurrentMaxUnitNumStatus = currentMaxUnitNum > minimumUnits

    # scaleIn综合条件
    scaleInMemoryCondition = scaleInMemoryConditionYARNMemoryAvailablePercentageStatus or scaleInMemoryConditionCapacityRemainingGBStatus
    scaleInPendingAppNumCondition = scaleInAppConditionPendingAppNumStatus
    scaleInCPULoadCondition = scaleInCPULoadStatus
    scaleIncurrentMaxUnitCondition = scaleIncurrentMaxUnitNumStatus
    scaleInCondition = (scaleInMemoryCondition or scaleInPendingAppNumCondition or scaleInCPULoadCondition) and scaleIncurrentMaxUnitCondition

    # 确定scaleStatus
    if scaleOutCondition and not scaleInCondition:
        return 1  # 执行scaleOut
    elif not scaleOutCondition and scaleInCondition:
        return -1  # 执行scaleIn
    else:
        return 0  # 无操作

# 示例调用，这里需要替换为实际参数
if __name__ == '__main__':
    scaleStatus = determine_scale_status(
        YARNMemoryAvailablePercentageList=[70, 80, 75],
        CapacityRemainingGBList=[100, 110, 120],
        pendingAppNumList=[5, 6, 7],
        taskNodeCPULoadList=[80, 85, 90],
        currentMaxUnitNum=100
    )
    logger.info(f"Scale Status: {scaleStatus}")
