import statistics
from tools.utils import Utils
from tools.ssm import AWSSSMClient
from tools.emr_ec2_metrics import NodeMetricsRetriever
from tools.emr_yarn import EMRMetricManager
from tools.emr import AWSEMRClient

# 配置loguru的logger
# Utils.logger.add("managed_scaling_enhanced.log",
#                  format="{time} {level} {message}", level="DEBUG")


class ManagedScalingEnhanced:
    def __init__(self, emr_id='j-1F74M1P9SC57B', prefix="managedScalingEnhanced"):
        self.emr_id = emr_id
        self.prefix = prefix
        self.ssm_client = AWSSSMClient()
        self.nodeMetrics_client = NodeMetricsRetriever()
        self.emr_metric_manager = EMRMetricManager()
        self.parameters = self._get_parameters()

    @Utils.exception_handler
    def get_current_max_unit_num(self):
        emr_client = AWSEMRClient()
        policy = emr_client.get_managed_scaling_policy(self.emr_id)
        compute_limits = policy.get(
            'ManagedScalingPolicy', {}).get('ComputeLimits', {})
        return compute_limits.get('MaximumCapacityUnits', 0)

    @Utils.exception_handler
    def _get_parameters(self):
        parameter_names = [
            f'/{self.prefix}/minimumUnits',
            f'/{self.prefix}/maximumUnits',
            f'/{self.prefix}/scaleOutAvgYARNMemoryAvailablePercentageValue',
            f'/{self.prefix}/scaleOutAvgCapacityRemainingGBValue',
            f'/{self.prefix}/scaleOutAvgPendingAppNumValue',
            f'/{self.prefix}/scaleOutAvgTaskNodeCPULoadValue',
            f'/{self.prefix}/scaleInAvgYARNMemoryAvailablePercentageValue',
            f'/{self.prefix}/scaleInAvgCapacityRemainingGBValue',
            f'/{self.prefix}/scaleInAvgPendingAppNumValue',
            f'/{self.prefix}/scaleInAvgTaskNodeCPULoadValue',
            f'/{self.prefix}/scaleOutAvgTaskNodeCPULoadMinutes',
            f'/{self.prefix}/scaleInAvgTaskNodeCPULoadMinutes',
        ]

        parameters = [self.ssm_client.get_parameters_from_parameter_store(
            i) for i in parameter_names]

        # 转换参数类型
        self.minimumUnits = int(parameters[0])
        self.maximumUnits = int(parameters[1])
        self.scaleOutAvgYARNMemoryAvailablePercentageValue = float(
            parameters[2])
        self.scaleOutAvgCapacityRemainingGBValue = float(parameters[3])
        self.scaleOutAvgPendingAppNumValue = float(parameters[4])
        self.scaleOutAvgTaskNodeCPULoadValue = float(parameters[5])
        self.scaleInAvgYARNMemoryAvailablePercentageValue = float(
            parameters[6])
        self.scaleInAvgCapacityRemainingGBValue = float(parameters[7])
        self.scaleInAvgPendingAppNumValue = float(parameters[8])
        self.scaleInAvgTaskNodeCPULoadValue = float(parameters[9])
        self.scaleOutAvgTaskNodeCPULoadMinutes = float(parameters[10])
        self.scaleInAvgTaskNodeCPULoadMinutes = float(parameters[11])

    @Utils.exception_handler
    def determine_scale_status(self):
        """
        根据输入的监控数据和当前Unit，决定是否扩缩容。
        """

        currentMaxUnitNum = self.get_current_max_unit_num()

        scaleOutYARNMemoryAvailablePercentageList = self.emr_metric_manager.get_data_from_sqlite(
            emr_cluster_id=self.emr_id,
            metric_name="YARNMemoryAvailablePercentage",
            prefix=self.prefix,
            ScaleStatus="scaleOut"
        )
        scaleOutCapacityRemainingGBList = self.emr_metric_manager.get_data_from_sqlite(
            emr_cluster_id=self.emr_id,
            metric_name="CapacityRemainingGB",
            prefix=self.prefix,
            ScaleStatus="scaleOut"
        )
        scaleOutpendingAppNumList = self.emr_metric_manager.get_data_from_sqlite(
            emr_cluster_id=self.emr_id,
            metric_name="PendingAppNum",
            prefix=self.prefix,
            ScaleStatus="scaleOut"
        )
        scaleOuttaskNodeCPULoadList = self.nodeMetrics_client.get_task_node_metrics(
            self.emr_id, instance_group_types_list=['TASK'], instance_states_list=['RUNNING'], window_minutes=self.scaleOutAvgTaskNodeCPULoadMinutes)

        scaleInYARNMemoryAvailablePercentageList = self.emr_metric_manager.get_data_from_sqlite(
            emr_cluster_id=self.emr_id,
            metric_name="YARNMemoryAvailablePercentage",
            prefix=self.prefix,
            ScaleStatus="scaleIn"
        )
        scaleInCapacityRemainingGBList = self.emr_metric_manager.get_data_from_sqlite(
            emr_cluster_id=self.emr_id,
            metric_name="CapacityRemainingGB",
            prefix=self.prefix,
            ScaleStatus="scaleIn"
        )
        scaleInpendingAppNumList = self.emr_metric_manager.get_data_from_sqlite(
            emr_cluster_id=self.emr_id,
            metric_name="PendingAppNum",
            prefix=self.prefix,
            ScaleStatus="scaleIn"
        )
        scaleIntaskNodeCPULoadList = self.nodeMetrics_client.get_task_node_metrics(
            self.emr_id, instance_group_types_list=['TASK'], instance_states_list=['RUNNING'], window_minutes=self.scaleInAvgTaskNodeCPULoadMinutes)

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
            scaleOutYARNMemoryAvailablePercentageList)*100 <= self.scaleOutAvgYARNMemoryAvailablePercentageValue
        Utils.logger.info(
            f"scaleOutMemoryConditionYARNMemoryAvailablePercentageStatus: {scaleOutMemoryConditionYARNMemoryAvailablePercentageStatus}, scaleOutYARNMemoryAvailablePercentageList mean: {statistics.mean(scaleOutYARNMemoryAvailablePercentageList)*100}, scaleOutAvgYARNMemoryAvailablePercentageValue: {self.scaleOutAvgYARNMemoryAvailablePercentageValue}")

        scaleOutMemoryConditionCapacityRemainingGBStatus = statistics.mean(
            scaleOutCapacityRemainingGBList) <= self.scaleOutAvgCapacityRemainingGBValue
        Utils.logger.info(
            f"scaleOutMemoryConditionCapacityRemainingGBStatus: {scaleOutMemoryConditionCapacityRemainingGBStatus}, scaleOutCapacityRemainingGBList mean: {statistics.mean(scaleOutCapacityRemainingGBList)}, scaleOutAvgCapacityRemainingGBValue: {self.scaleOutAvgCapacityRemainingGBValue}")

        scaleOutAppConditionPendingAppNumStatus = statistics.mean(
            scaleOutpendingAppNumList) >= self.scaleOutAvgPendingAppNumValue
        Utils.logger.info(
            f"scaleOutAppConditionPendingAppNumStatus: {scaleOutAppConditionPendingAppNumStatus}, scaleOutpendingAppNumList mean: {statistics.mean(scaleOutpendingAppNumList)}, scaleOutAvgPendingAppNumValue: {self.scaleOutAvgPendingAppNumValue}")

        scaleOutCPULoadStatus = statistics.mean(
            scaleOuttaskNodeCPULoadList) >= self.scaleOutAvgTaskNodeCPULoadValue
        Utils.logger.info(
            f"scaleOutCPULoadStatus: {scaleOutCPULoadStatus}, scaleOuttaskNodeCPULoadList mean: {statistics.mean(scaleOuttaskNodeCPULoadList)}, scaleOutAvgTaskNodeCPULoadValue: {self.scaleOutAvgTaskNodeCPULoadValue}")

        scaleOutcurrentMaxUnitNumStatus = currentMaxUnitNum < self.maximumUnits
        Utils.logger.info(
            f"scaleOutcurrentMaxUnitNumStatus: {scaleOutcurrentMaxUnitNumStatus}, currentMaxUnitNum: {currentMaxUnitNum}, maximumUnits: {self.maximumUnits}")

        # scaleOut综合条件
        scaleOutMemoryCondition = scaleOutMemoryConditionYARNMemoryAvailablePercentageStatus or scaleOutMemoryConditionCapacityRemainingGBStatus
        Utils.logger.info(
            f"scaleOutMemoryCondition: {scaleOutMemoryCondition}")

        scaleOutPendingAppNumCondition = scaleOutAppConditionPendingAppNumStatus
        Utils.logger.info(
            f"scaleOutPendingAppNumCondition: {scaleOutPendingAppNumCondition}")

        scaleOutCPULoadCondition = scaleOutCPULoadStatus
        Utils.logger.info(
            f"scaleOutCPULoadCondition: {scaleOutCPULoadCondition}")

        scaleOutcurrentMaxUnitCondition = scaleOutcurrentMaxUnitNumStatus
        Utils.logger.info(
            f"scaleOutcurrentMaxUnitCondition: {scaleOutcurrentMaxUnitCondition}")

        scaleOutCondition = scaleOutMemoryCondition and scaleOutPendingAppNumCondition and scaleOutCPULoadCondition and scaleOutcurrentMaxUnitCondition
        Utils.logger.info(f"scaleOutCondition: {scaleOutCondition}")

        # scaleIn单项条件状态 🐒
        scaleInMemoryConditionYARNMemoryAvailablePercentageStatus = statistics.mean(
            scaleInYARNMemoryAvailablePercentageList)*100 > self.scaleInAvgYARNMemoryAvailablePercentageValue
        Utils.logger.info(
            f"scaleInMemoryConditionYARNMemoryAvailablePercentageStatus: {scaleInMemoryConditionYARNMemoryAvailablePercentageStatus}, scaleInYARNMemoryAvailablePercentageList mean: {statistics.mean(scaleInYARNMemoryAvailablePercentageList)*100}, scaleInAvgYARNMemoryAvailablePercentageValue: {self.scaleInAvgYARNMemoryAvailablePercentageValue}")

        scaleInMemoryConditionCapacityRemainingGBStatus = statistics.mean(
            scaleInCapacityRemainingGBList) > self.scaleInAvgCapacityRemainingGBValue
        Utils.logger.info(
            f"scaleInMemoryConditionCapacityRemainingGBStatus: {scaleInMemoryConditionCapacityRemainingGBStatus}, scaleInCapacityRemainingGBList mean: {statistics.mean(scaleInCapacityRemainingGBList)}, scaleInAvgCapacityRemainingGBValue: {self.scaleInAvgCapacityRemainingGBValue}")

        scaleInAppConditionPendingAppNumStatus = statistics.mean(
            scaleInpendingAppNumList) < self.scaleInAvgPendingAppNumValue
        Utils.logger.info(
            f"scaleInAppConditionPendingAppNumStatus: {scaleInAppConditionPendingAppNumStatus}, scaleInpendingAppNumList mean: {statistics.mean(scaleInpendingAppNumList)}, scaleInAvgPendingAppNumValue: {self.scaleInAvgPendingAppNumValue}")

        scaleInCPULoadStatus = statistics.mean(
            scaleIntaskNodeCPULoadList) < self.scaleInAvgTaskNodeCPULoadValue
        Utils.logger.info(
            f"scaleInCPULoadStatus: {scaleInCPULoadStatus}, scaleIntaskNodeCPULoadList mean: {statistics.mean(scaleIntaskNodeCPULoadList)}, scaleInAvgTaskNodeCPULoadValue: {self.scaleInAvgTaskNodeCPULoadValue}")

        scaleIncurrentMaxUnitNumStatus = currentMaxUnitNum > self.minimumUnits
        Utils.logger.info(
            f"scaleIncurrentMaxUnitNumStatus: {scaleIncurrentMaxUnitNumStatus}, currentMaxUnitNum: {currentMaxUnitNum}, minimumUnits: {self.minimumUnits}")

        # scaleIn综合条件
        scaleInMemoryCondition = scaleInMemoryConditionYARNMemoryAvailablePercentageStatus or scaleInMemoryConditionCapacityRemainingGBStatus
        Utils.logger.info(f"scaleInMemoryCondition: {scaleInMemoryCondition}")

        scaleInPendingAppNumCondition = scaleInAppConditionPendingAppNumStatus
        Utils.logger.info(
            f"scaleInPendingAppNumCondition: {scaleInPendingAppNumCondition}")

        scaleInCPULoadCondition = scaleInCPULoadStatus
        Utils.logger.info(
            f"scaleInCPULoadCondition: {scaleInCPULoadCondition}")

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

    def scale_out(self):
        # 实现scale_out逻辑
        pass

    def scale_in(self):
        # 实现scale_in逻辑
        pass
