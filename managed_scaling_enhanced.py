
from functools import wraps
import statistics
from tools.utils import Utils
from tools.ssm import AWSSSMClient

# 配置loguru的logger
Utils.logger.add("managed_scaling_enhanced.log",
                 format="{time} {level} {message}", level="DEBUG")


@Utils.exception_handler
def determine_scale_status(YARNMemoryAvailablePercentageList, CapacityRemainingGBList, pendingAppNumList, taskNodeCPULoadList, currentMaxUnitNum):
    """
    根据输入的监控数据和当前最大单元数，决定是否扩缩容。
    """
    # 定义前缀字符串变量
    ssm_client = AWSSSMClient()
    prefix = "managedScalingEnhanced"

    minimumUnits = int(ssm_client.get_parameters_from_parameter_store(
        f'/{prefix}/minimumUnits'))
    maximumUnits = int(ssm_client.get_parameters_from_parameter_store(
        f'/{prefix}/maximumUnits'))
    scaleOutAvgYARNMemoryAvailablePercentageValue = float(ssm_client.get_parameters_from_parameter_store(
        f'/{prefix}/scaleOutAvgYARNMemoryAvailablePercentageValue'))
    scaleOutAvgCapacityRemainingGBValue = float(ssm_client.get_parameters_from_parameter_store(
        f'/{prefix}/scaleOutAvgCapacityRemainingGBValue'))
    scaleOutAvgPendingAppNumValue = float(ssm_client.get_parameters_from_parameter_store(
        f'/{prefix}/scaleOutAvgPendingAppNumValue'))
    scaleOutAvgTaskNodeCPULoadValue = float(ssm_client.get_parameters_from_parameter_store(
        f'/{prefix}/scaleOutAvgTaskNodeCPULoadValue'))
    scaleInAvgYARNMemoryAvailablePercentageValue = float(ssm_client.get_parameters_from_parameter_store(
        f'/{prefix}/scaleInAvgYARNMemoryAvailablePercentageValue'))
    scaleInAvgCapacityRemainingGBValue = float(ssm_client.get_parameters_from_parameter_store(
        f'/{prefix}/scaleInAvgCapacityRemainingGBValue'))
    scaleInAvgPendingAppNumValue = float(ssm_client.get_parameters_from_parameter_store(
        f'/{prefix}/scaleInAvgPendingAppNumValue'))
    scaleInAvgTaskNodeCPULoadValue = float(ssm_client.get_parameters_from_parameter_store(
        f'/{prefix}/scaleInAvgTaskNodeCPULoadValue'))

    # 判断逻辑
    # scaleOut单项条件状态
    scaleOutMemoryConditionYARNMemoryAvailablePercentageStatus = statistics.mean(
        YARNMemoryAvailablePercentageList) <= scaleOutAvgYARNMemoryAvailablePercentageValue
    scaleOutMemoryConditionCapacityRemainingGBStatus = statistics.mean(
        CapacityRemainingGBList) <= scaleOutAvgCapacityRemainingGBValue
    scaleOutAppConditionPendingAppNumStatus = statistics.mean(
        pendingAppNumList) >= scaleOutAvgPendingAppNumValue
    scaleOutCPULoadStatus = statistics.mean(
        taskNodeCPULoadList) >= scaleOutAvgTaskNodeCPULoadValue
    scaleOutcurrentMaxUnitNumStatus = currentMaxUnitNum < maximumUnits

    # scaleOut综合条件
    scaleOutMemoryCondition = scaleOutMemoryConditionYARNMemoryAvailablePercentageStatus or scaleOutMemoryConditionCapacityRemainingGBStatus
    scaleOutPendingAppNumCondition = scaleOutAppConditionPendingAppNumStatus
    scaleOutCPULoadCondition = scaleOutCPULoadStatus
    scaleOutcurrentMaxUnitCondition = scaleOutcurrentMaxUnitNumStatus
    scaleOutCondition = scaleOutMemoryCondition and scaleOutPendingAppNumCondition and scaleOutCPULoadCondition and scaleOutcurrentMaxUnitCondition

    # scaleIn单项条件状态
    scaleInMemoryConditionYARNMemoryAvailablePercentageStatus = statistics.mean(
        YARNMemoryAvailablePercentageList) > scaleInAvgYARNMemoryAvailablePercentageValue
    scaleInMemoryConditionCapacityRemainingGBStatus = statistics.mean(
        CapacityRemainingGBList) > scaleInAvgCapacityRemainingGBValue
    scaleInAppConditionPendingAppNumStatus = statistics.mean(
        pendingAppNumList) < scaleInAvgPendingAppNumValue
    scaleInCPULoadStatus = statistics.mean(
        taskNodeCPULoadList) < scaleInAvgTaskNodeCPULoadValue
    scaleIncurrentMaxUnitNumStatus = currentMaxUnitNum > minimumUnits

    # scaleIn综合条件
    scaleInMemoryCondition = scaleInMemoryConditionYARNMemoryAvailablePercentageStatus or scaleInMemoryConditionCapacityRemainingGBStatus
    scaleInPendingAppNumCondition = scaleInAppConditionPendingAppNumStatus
    scaleInCPULoadCondition = scaleInCPULoadStatus
    scaleIncurrentMaxUnitCondition = scaleIncurrentMaxUnitNumStatus
    scaleInCondition = (
        scaleInMemoryCondition or scaleInPendingAppNumCondition or scaleInCPULoadCondition) and scaleIncurrentMaxUnitCondition

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
    Utils.logger.info(f"Scale Status: {scaleStatus}")
