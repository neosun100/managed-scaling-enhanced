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

    parameters = {
        'minimumTaskUnits': int,
        'maximumUnits': int,

        'spotInstancesTimeout': int,

        'monitorIntervalSeconds': int,
        'actionIntervalSeconds': int,

        'scaleOutAvgYARNMemoryAvailablePercentageValue': float,
        'scaleOutAvgYARNMemoryAvailablePercentageMinutes': int,
        'scaleOutAvgCapacityRemainingGBValue': float,
        'scaleOutAvgCapacityRemainingGBMinutes': int,
        'scaleOutAvgPendingAppNumValue': float,
        'scaleOutAvgPendingAppNumMinutes': int,
        'scaleOutAvgTaskNodeCPULoadValue': float,
        'scaleOutAvgTaskNodeCPULoadMinutes': int,

        'scaleInAvgYARNMemoryAvailablePercentageValue': float,
        'scaleInAvgYARNMemoryAvailablePercentageMinutes': int,
        'scaleInAvgCapacityRemainingGBValue': float,
        'scaleInAvgCapacityRemainingGBMinutes': int,
        'scaleInAvgPendingAppNumValue': float,
        'scaleInAvgPendingAppNumMinutes': int,
        'scaleInAvgTaskNodeCPULoadValue': float,
        'scaleInAvgTaskNodeCPULoadMinutes': int
    }

    for param_name, conversion_func in parameters.items():
        param_value = conversion_func(
            ssm_client.get_parameters_from_parameter_store(f'/{prefix}/{param_name}'))
        locals()[param_name] = param_value

    currentMaxUnitNum = 100

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
        metric_name="pendingAppNum",
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
        metric_name="pendingAppNum",
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
        scaleOutYARNMemoryAvailablePercentageList) <= scaleOutAvgYARNMemoryAvailablePercentageValue
    scaleOutMemoryConditionCapacityRemainingGBStatus = statistics.mean(
        scaleOutCapacityRemainingGBList) <= scaleOutAvgCapacityRemainingGBValue
    scaleOutAppConditionPendingAppNumStatus = statistics.mean(
        scaleOutpendingAppNumList) >= scaleOutAvgPendingAppNumValue
    scaleOutCPULoadStatus = statistics.mean(
        scaleOuttaskNodeCPULoadList) >= scaleOutAvgTaskNodeCPULoadValue
    scaleOutcurrentMaxUnitNumStatus = currentMaxUnitNum < maximumUnits

    # scaleOut综合条件
    scaleOutMemoryCondition = scaleOutMemoryConditionYARNMemoryAvailablePercentageStatus or scaleOutMemoryConditionCapacityRemainingGBStatus
    scaleOutPendingAppNumCondition = scaleOutAppConditionPendingAppNumStatus
    scaleOutCPULoadCondition = scaleOutCPULoadStatus
    scaleOutcurrentMaxUnitCondition = scaleOutcurrentMaxUnitNumStatus
    scaleOutCondition = scaleOutMemoryCondition and scaleOutPendingAppNumCondition and scaleOutCPULoadCondition and scaleOutcurrentMaxUnitCondition

    # scaleIn单项条件状态 🐒
    scaleInMemoryConditionYARNMemoryAvailablePercentageStatus = statistics.mean(
        scaleInYARNMemoryAvailablePercentageList) > scaleInAvgYARNMemoryAvailablePercentageValue
    scaleInMemoryConditionCapacityRemainingGBStatus = statistics.mean(
        scaleInCapacityRemainingGBList) > scaleInAvgCapacityRemainingGBValue
    scaleInAppConditionPendingAppNumStatus = statistics.mean(
        scaleInpendingAppNumList) < scaleInAvgPendingAppNumValue
    scaleInCPULoadStatus = statistics.mean(
        scaleIntaskNodeCPULoadList) < scaleInAvgTaskNodeCPULoadValue
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
        emr_id='j-1F74M1P9SC57B', prefix="managedScalingEnhanced"
    )
    Utils.logger.info(f"Scale Status: {scaleStatus}")
