import statistics
from tools.utils import Utils
from tools.ssm import AWSSSMClient
from tools.emr_ec2_metrics import NodeMetricsRetriever
from tools.emr_yarn import EMRMetricManager

# 配置loguru的logger
Utils.logger.add("managed_scaling_enhanced.log",
                 format="{time} {level} {message}", level="DEBUG")


@Utils.exception_handler
def determine_scale_status(emr_id='j-1F74M1P9SC57B', prefix="managedScalingEnhanced"):
    """
    根据输入的监控数据和当前Unit，决定是否扩缩容。
    """
    # 定义前缀字符串变量

    ssm_client = AWSSSMClient()

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
        f'/{prefix}/scaleOutAvgTaskNodeCPULoadMinutes',
        f'/{prefix}/scaleInAvgTaskNodeCPULoadMinutes',
    ]

    parameters = [ssm_client.get_parameters_from_parameter_store(
        i) for i in parameter_names]

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
    scaleOutAvgTaskNodeCPULoadMinutes = float(parameters[10])
    scaleInAvgTaskNodeCPULoadMinutes = float(parameters[11])

    currentMaxUnitNum = 640

    nodeMetrics_client = NodeMetricsRetriever()
    emr_metric_manager = EMRMetricManager()

    scaleOutYARNMemoryAvailablePercentageList = emr_metric_manager.get_data_from_sqlite(
        emr_cluster_id=emr_id,
        metric_name="YARNMemoryAvailablePercentage",
        prefix=prefix,
        ScaleStatus="scaleOut"
    )
    scaleOutCapacityRemainingGBList = emr_metric_manager.get_data_from_sqlite(
        emr_cluster_id=emr_id,
        metric_name="CapacityRemainingGB",
        prefix=prefix,
        ScaleStatus="scaleOut"
    )
    scaleOutpendingAppNumList = emr_metric_manager.get_data_from_sqlite(
        emr_cluster_id=emr_id,
        metric_name="PendingAppNum",
        prefix=prefix,
        ScaleStatus="scaleOut"
    )
    scaleOuttaskNodeCPULoadList = nodeMetrics_client.get_task_node_metrics(
        emr_id, instance_group_types_list=['TASK'], instance_states_list=['RUNNING'], window_minutes=scaleOutAvgTaskNodeCPULoadMinutes)

    scaleInYARNMemoryAvailablePercentageList = emr_metric_manager.get_data_from_sqlite(
        emr_cluster_id=emr_id,
        metric_name="YARNMemoryAvailablePercentage",
        prefix=prefix,
        ScaleStatus="scaleIn"
    )
    scaleInCapacityRemainingGBList = emr_metric_manager.get_data_from_sqlite(
        emr_cluster_id=emr_id,
        metric_name="CapacityRemainingGB",
        prefix=prefix,
        ScaleStatus="scaleIn"
    )
    scaleInpendingAppNumList = emr_metric_manager.get_data_from_sqlite(
        emr_cluster_id=emr_id,
        metric_name="PendingAppNum",
        prefix=prefix,
        ScaleStatus="scaleIn"
    )
    scaleIntaskNodeCPULoadList = nodeMetrics_client.get_task_node_metrics(
        emr_id, instance_group_types_list=['TASK'], instance_states_list=['RUNNING'], window_minutes=scaleInAvgTaskNodeCPULoadMinutes)

    monitoring_data_lists = [
        scaleOutYARNMemoryAvailablePercentageList,
        scaleOutCapacityRemainingGBList,
        scaleOutpendingAppNumList,
        scaleOuttaskNodeCPULoadList,
        scaleInYARNMemoryAvailablePercentageList,
        scaleInCapacityRemainingGBList,
        scaleInpendingAppNumList,
        scaleIntaskNodeCPULoadList
    ]

    if not all(monitoring_data_lists):
        Utils.logger.info(
            "At least one of the input monitoring data lists is empty, so no scaling operations will be performed.")
        return 0

    # 判断逻辑
    # scaleOut单项条件状态 🐒
    scaleOutMemoryConditionYARNMemoryAvailablePercentageStatus = statistics.mean(
        scaleOutYARNMemoryAvailablePercentageList)*100 <= scaleOutAvgYARNMemoryAvailablePercentageValue
    Utils.logger.info(
        f"scaleOutMemoryConditionYARNMemoryAvailablePercentageStatus: {scaleOutMemoryConditionYARNMemoryAvailablePercentageStatus}, scaleOutYARNMemoryAvailablePercentageList mean: {statistics.mean(scaleOutYARNMemoryAvailablePercentageList)*100}, scaleOutAvgYARNMemoryAvailablePercentageValue: {scaleOutAvgYARNMemoryAvailablePercentageValue}")

    scaleOutMemoryConditionCapacityRemainingGBStatus = statistics.mean(
        scaleOutCapacityRemainingGBList) <= scaleOutAvgCapacityRemainingGBValue
    Utils.logger.info(
        f"scaleOutMemoryConditionCapacityRemainingGBStatus: {scaleOutMemoryConditionCapacityRemainingGBStatus}, scaleOutCapacityRemainingGBList mean: {statistics.mean(scaleOutCapacityRemainingGBList)}, scaleOutAvgCapacityRemainingGBValue: {scaleOutAvgCapacityRemainingGBValue}")

    scaleOutAppConditionPendingAppNumStatus = statistics.mean(
        scaleOutpendingAppNumList) >= scaleOutAvgPendingAppNumValue
    Utils.logger.info(
        f"scaleOutAppConditionPendingAppNumStatus: {scaleOutAppConditionPendingAppNumStatus}, scaleOutpendingAppNumList mean: {statistics.mean(scaleOutpendingAppNumList)}, scaleOutAvgPendingAppNumValue: {scaleOutAvgPendingAppNumValue}")

    scaleOutCPULoadStatus = statistics.mean(
        scaleOuttaskNodeCPULoadList) >= scaleOutAvgTaskNodeCPULoadValue
    Utils.logger.info(
        f"scaleOutCPULoadStatus: {scaleOutCPULoadStatus}, scaleOuttaskNodeCPULoadList mean: {statistics.mean(scaleOuttaskNodeCPULoadList)}, scaleOutAvgTaskNodeCPULoadValue: {scaleOutAvgTaskNodeCPULoadValue}")

    scaleOutcurrentMaxUnitNumStatus = currentMaxUnitNum < maximumUnits
    Utils.logger.info(
        f"scaleOutcurrentMaxUnitNumStatus: {scaleOutcurrentMaxUnitNumStatus}, currentMaxUnitNum: {currentMaxUnitNum}, maximumUnits: {maximumUnits}")

    # scaleOut综合条件
    scaleOutMemoryCondition = scaleOutMemoryConditionYARNMemoryAvailablePercentageStatus or scaleOutMemoryConditionCapacityRemainingGBStatus
    Utils.logger.info(f"scaleOutMemoryCondition: {scaleOutMemoryCondition}")

    scaleOutPendingAppNumCondition = scaleOutAppConditionPendingAppNumStatus
    Utils.logger.info(
        f"scaleOutPendingAppNumCondition: {scaleOutPendingAppNumCondition}")

    scaleOutCPULoadCondition = scaleOutCPULoadStatus
    Utils.logger.info(f"scaleOutCPULoadCondition: {scaleOutCPULoadCondition}")

    scaleOutcurrentMaxUnitCondition = scaleOutcurrentMaxUnitNumStatus
    Utils.logger.info(
        f"scaleOutcurrentMaxUnitCondition: {scaleOutcurrentMaxUnitCondition}")

    scaleOutCondition = scaleOutMemoryCondition and scaleOutPendingAppNumCondition and scaleOutCPULoadCondition and scaleOutcurrentMaxUnitCondition
    Utils.logger.info(f"scaleOutCondition: {scaleOutCondition}")

    # scaleIn单项条件状态 🐒
    scaleInMemoryConditionYARNMemoryAvailablePercentageStatus = statistics.mean(
        scaleInYARNMemoryAvailablePercentageList)*100 > scaleInAvgYARNMemoryAvailablePercentageValue
    Utils.logger.info(
        f"scaleInMemoryConditionYARNMemoryAvailablePercentageStatus: {scaleInMemoryConditionYARNMemoryAvailablePercentageStatus}, scaleInYARNMemoryAvailablePercentageList mean: {statistics.mean(scaleInYARNMemoryAvailablePercentageList)*100}, scaleInAvgYARNMemoryAvailablePercentageValue: {scaleInAvgYARNMemoryAvailablePercentageValue}")

    scaleInMemoryConditionCapacityRemainingGBStatus = statistics.mean(
        scaleInCapacityRemainingGBList) > scaleInAvgCapacityRemainingGBValue
    Utils.logger.info(
        f"scaleInMemoryConditionCapacityRemainingGBStatus: {scaleInMemoryConditionCapacityRemainingGBStatus}, scaleInCapacityRemainingGBList mean: {statistics.mean(scaleInCapacityRemainingGBList)}, scaleInAvgCapacityRemainingGBValue: {scaleInAvgCapacityRemainingGBValue}")

    scaleInAppConditionPendingAppNumStatus = statistics.mean(
        scaleInpendingAppNumList) < scaleInAvgPendingAppNumValue
    Utils.logger.info(
        f"scaleInAppConditionPendingAppNumStatus: {scaleInAppConditionPendingAppNumStatus}, scaleInpendingAppNumList mean: {statistics.mean(scaleInpendingAppNumList)}, scaleInAvgPendingAppNumValue: {scaleInAvgPendingAppNumValue}")

    scaleInCPULoadStatus = statistics.mean(
        scaleIntaskNodeCPULoadList) < scaleInAvgTaskNodeCPULoadValue
    Utils.logger.info(
        f"scaleInCPULoadStatus: {scaleInCPULoadStatus}, scaleIntaskNodeCPULoadList mean: {statistics.mean(scaleIntaskNodeCPULoadList)}, scaleInAvgTaskNodeCPULoadValue: {scaleInAvgTaskNodeCPULoadValue}")

    scaleIncurrentMaxUnitNumStatus = currentMaxUnitNum > minimumUnits
    Utils.logger.info(
        f"scaleIncurrentMaxUnitNumStatus: {scaleIncurrentMaxUnitNumStatus}, currentMaxUnitNum: {currentMaxUnitNum}, minimumUnits: {minimumUnits}")

    # scaleIn综合条件
    scaleInMemoryCondition = scaleInMemoryConditionYARNMemoryAvailablePercentageStatus or scaleInMemoryConditionCapacityRemainingGBStatus
    Utils.logger.info(f"scaleInMemoryCondition: {scaleInMemoryCondition}")

    scaleInPendingAppNumCondition = scaleInAppConditionPendingAppNumStatus
    Utils.logger.info(
        f"scaleInPendingAppNumCondition: {scaleInPendingAppNumCondition}")

    scaleInCPULoadCondition = scaleInCPULoadStatus
    Utils.logger.info(f"scaleInCPULoadCondition: {scaleInCPULoadCondition}")

    scaleIncurrentMaxUnitCondition = scaleIncurrentMaxUnitNumStatus
    Utils.logger.info(
        f"scaleIncurrentMaxUnitCondition: {scaleIncurrentMaxUnitCondition}")

    scaleInCondition = (
        scaleInMemoryCondition or scaleInPendingAppNumCondition or scaleInCPULoadCondition) and scaleIncurrentMaxUnitCondition
    Utils.logger.info(f"scaleInCondition: {scaleInCondition}")

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
        emr_id='j-1F74M1P9SC57B', prefix="managedScalingEnhanced"
    )
    Utils.logger.info(f"Scale Status: {scaleStatus}")
