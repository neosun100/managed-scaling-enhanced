import statistics
import asyncio
from tools.utils import Utils
from tools.ssm import AWSSSMClient
from tools.emr import AWSEMRClient

# 配置loguru的logger
Utils.logger.add("managed_scaling_enhanced.log",
                 format="{time} {level} {message}", level="DEBUG")


emrID = 'j-1F74M1P9SC57B'

@Utils.exception_handler
async def determine_scale_status(YARNMemoryAvailablePercentageList, CapacityRemainingGBList, pendingAppNumList, taskNodeCPULoadList, currentMaxUnitNum):
    """
    根据输入的监控数据和当前最大单元数，决定是否扩缩容。
    """
    # 定义前缀字符串变量

    # 检查输入的监控数据列表是否都非空
    if not YARNMemoryAvailablePercentageList or not CapacityRemainingGBList or not pendingAppNumList or not taskNodeCPULoadList:
        Utils.logger.info(
            "At least one of the input monitoring data lists is empty, so no scaling operations will be performed.")
        Utils.logger.info(
            f"Determining scale status with inputs: YARNMemoryAvailablePercentageList={YARNMemoryAvailablePercentageList}, CapacityRemainingGBList={CapacityRemainingGBList}, pendingAppNumList={pendingAppNumList}, taskNodeCPULoadList={taskNodeCPULoadList}, currentMaxUnitNum={currentMaxUnitNum}")
        return 0


    ssm_client = AWSSSMClient()
    prefix = "managedScalingEnhanced"

    parameter_names = [
    f'/{prefix}/minimumUnits',
    f'/{prefix}/maximumUnits',
    f'/{prefix}/scaleOutAvgYARNMemoryAvailablePercentageValue',
    f'/{prefix}/scaleOutAvgCapacityRemainingGBValue',
    f'/{prefix}/scaleOutAvgPendingAppNumValue',
    f'/{prefix}/scaleOutAvgTaskNodeCPULoadValue',
    f'/{prefix}/scaleInAvgYARNMemoryAvailablePercentageValue',
    f'/{prefix}/scaleInAvgCapacityRemainingGBValue',
    f'/{prefix}/scaleInAvgPendingAppNumValue',
    f'/{prefix}/scaleInAvgTaskNodeCPULoadValue',
]
    # 使用一次性获取所有参数的异步方法
    parameters = await ssm_client.aioget_parameters_from_parameter_store(parameter_names)

    # 转换参数类型
    minimumUnits = int(parameters[0])
    maximumUnits = int(parameters[1])
    scaleOutAvgYARNMemoryAvailablePercentageValue = float(parameters[2])
    scaleOutAvgCapacityRemainingGBValue = float(parameters[3])
    scaleOutAvgPendingAppNumValue = float(parameters[4])
    scaleOutAvgTaskNodeCPULoadValue = float(parameters[5])
    scaleInAvgYARNMemoryAvailablePercentageValue = float(parameters[6])
    scaleInAvgCapacityRemainingGBValue = float(parameters[7])
    scaleInAvgPendingAppNumValue = float(parameters[8])
    scaleInAvgTaskNodeCPULoadValue = float(parameters[9])


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
    scaleStatus = asyncio.run(determine_scale_status(
        YARNMemoryAvailablePercentageList=[70, 80, 75],
        CapacityRemainingGBList=[100, 110, 120],
        pendingAppNumList=[5, 6, 7],
        taskNodeCPULoadList=[],
        currentMaxUnitNum=100
    ))
    Utils.logger.info(f"Scale Status: {scaleStatus}")
