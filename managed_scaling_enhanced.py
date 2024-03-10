import statistics
import sqlite3
from datetime import datetime, timedelta
from tools.utils import Utils
from tools.ssm import AWSSSMClient
from tools.emr_ec2_metrics import NodeMetricsRetriever
from tools.emr_yarn import EMRMetricManager
from tools.emr import AWSEMRClient

# 配置loguru的logger
# Utils.logger.add("managed_scaling_enhanced.log",
#                  format="{time} {level} {message}", level="DEBUG")


class ManagedScalingEnhanced:
    def __init__(self, emr_id='j-1F74M1P9SC57B', prefix="managedScalingEnhanced",spot_switch_on_demand=0):
        self.emr_id = emr_id
        self.prefix = prefix
        self.spot_switch_on_demand = spot_switch_on_demand
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
            f'/{self.prefix}/scaleOutFactor',
            f'/{self.prefix}/scaleInFactor',
            f'/{self.prefix}/spotInstancesTimeout',
            f'/{self.prefix}/maximumOnDemandInstancesNumValue',
        ]

        parameters = [self.ssm_client.get_parameters_from_parameter_store(i) for i in parameter_names]

        # 转换参数类型
        self.minimumUnits = int(parameters[0])
        self.maximumUnits = int(parameters[1])
        self.scaleOutAvgYARNMemoryAvailablePercentageValue = float(parameters[2])
        self.scaleOutAvgCapacityRemainingGBValue = float(parameters[3])
        self.scaleOutAvgPendingAppNumValue = float(parameters[4])
        self.scaleOutAvgTaskNodeCPULoadValue = float(parameters[5])
        self.scaleInAvgYARNMemoryAvailablePercentageValue = float(parameters[6])
        self.scaleInAvgCapacityRemainingGBValue = float(parameters[7])
        self.scaleInAvgPendingAppNumValue = float(parameters[8])
        self.scaleInAvgTaskNodeCPULoadValue = float(parameters[9])
        self.scaleOutAvgTaskNodeCPULoadMinutes = float(parameters[10])
        self.scaleInAvgTaskNodeCPULoadMinutes = float(parameters[11])
        self.scaleOutFactor = float(parameters[12])
        self.scaleInFactor = float(parameters[13])
        self.spotInstancesTimeout = int(parameters[14])
        self.maximumOnDemandInstancesNumValue = int(parameters[15])



    @staticmethod
    @Utils.exception_handler
    def sanitize_table_name(table_name):
        """
        替换表名中的非法字符。

        :param table_name: 原始表名
        :return: 经过处理的表名
        """
        return ''.join(c if c.isalnum() or c == '_' else '_' for c in table_name)

    @Utils.exception_handler
    def determine_scale_status(self):
        """
        根据输入的监控数据和当前Unit，决定是否扩缩容。
        """

        currentMaxUnitNum = self.get_current_max_unit_num()
        Utils.logger.info(
            f"The current cluster's Maximum Capacity Units value: {currentMaxUnitNum}")

        Utils.logger.info(
            "Obtain relevant metrics for scaleOut.⬆️ ⬆️ ⬆️ ⬆️ ⬆️ ⬆️ ⬆️")

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

        Utils.logger.info(
            "Obtain relevant metrics for scaleOut.⬇️ ⬇️ ⬇️ ⬇️ ⬇️ ⬇️ ⬇️")

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

    @Utils.exception_handler
    def scale_out(self):
        pending_virtual_cores = self.emr_metric_manager.get_yarn_metrics(self.emr_id, 'pendingVirtualCores')
        apps_pending = self.emr_metric_manager.get_yarn_metrics(self.emr_id, 'appsPending')

        # 如果 apps_pending 为 0,则直接返回
        if apps_pending == 0:
            Utils.logger.info("No pending applications, skipping scale out operation.")
            return

        # 获取当前策略
        emr_client = AWSEMRClient()
        current_policy = emr_client.get_managed_scaling_policy(self.emr_id)
        current_max_capacity_units = current_policy['ManagedScalingPolicy']['ComputeLimits']['MaximumCapacityUnits']
        current_min_capacity_units = current_policy['ManagedScalingPolicy']['ComputeLimits']['MinimumCapacityUnits']

        # 计算新的 MaximumCapacityUnits
        new_max_capacity_units = current_max_capacity_units + int(
            (pending_virtual_cores / apps_pending) * self.scaleOutFactor
        )
        Utils.logger.info(f"init current_max_capacity_units: {current_max_capacity_units}")
        Utils.logger.info(f"init pending_virtual_cores: {pending_virtual_cores}")
        Utils.logger.info(f"init self.scaleOutFactor: {self.scaleOutFactor}")
        Utils.logger.info(f"init apps_pending: {apps_pending}")        
        Utils.logger.info(f"init new_max_capacity_units: {new_max_capacity_units}")

        # 确保新的 MaximumCapacityUnits 大于 MinimumCapacityUnits
        new_max_capacity_units = max(new_max_capacity_units, current_min_capacity_units + 1)

        # 确保新的 MaximumCapacityUnits 不超过最大限制
        new_max_capacity_units = min(new_max_capacity_units, self.maximumUnits)

        # 更新 MaximumCapacityUnits
        current_policy['ManagedScalingPolicy']['ComputeLimits']['MaximumCapacityUnits'] = new_max_capacity_units

        # 应用新策略
        emr_client.put_managed_scaling_policy(self.emr_id, current_policy['ManagedScalingPolicy'])

        # 记录新策略到 SQLite
        sanitized_table_name = self.sanitize_table_name(f"{self.emr_id}_ms_invoke_log")
        conn = sqlite3.connect(f"{sanitized_table_name}.db")
        c = conn.cursor()
        c.execute(f"CREATE TABLE IF NOT EXISTS {sanitized_table_name} (Timestamp INTEGER PRIMARY KEY, MaximumCapacityUnits INTEGER)")
        c.execute(f"INSERT INTO {sanitized_table_name} (Timestamp, MaximumCapacityUnits) VALUES (?, ?)", (int(datetime.now().timestamp()), new_max_capacity_units))
        conn.commit()
        conn.close()

        Utils.logger.info(f"New policy applied: {current_policy['ManagedScalingPolicy']}")

        # 检查是否需要补充 On-Demand 实例
        if self.spot_switch_on_demand == 1:
            timeout_timestamp = datetime.now() - timedelta(seconds=self.spotInstancesTimeout)
            conn = sqlite3.connect(f"{sanitized_table_name}.db")
            c = conn.cursor()
            c.execute(f"SELECT MIN(MaximumCapacityUnits) FROM {sanitized_table_name} WHERE Timestamp >= ?", (int(timeout_timestamp.timestamp()),))
            min_max_capacity_units = c.fetchone()[0]
            conn.close()

            total_virtual_cores = self.emr_metric_manager.get_yarn_metrics(self.emr_id, 'totalVirtualCores')
            if min_max_capacity_units > total_virtual_cores:
                # 需要补充 On-Demand 实例
                on_demand_units_to_add = min_max_capacity_units - total_virtual_cores
                current_policy['ManagedScalingPolicy']['ComputeLimits']['MaximumOnDemandCapacityUnits'] += on_demand_units_to_add
                emr_client.put_managed_scaling_policy(self.emr_id, current_policy['ManagedScalingPolicy'])
                Utils.logger.info(f"Added {on_demand_units_to_add} On-Demand units to the policy.")


    @Utils.exception_handler
    def scale_in(self):
        # 获取 YARN 指标
        pending_virtual_cores = self.emr_metric_manager.get_yarn_metrics(self.emr_id, 'pendingVirtualCores')
        apps_pending = self.emr_metric_manager.get_yarn_metrics(self.emr_id, 'appsPending')

        # 获取当前策略
        emr_client = AWSEMRClient()
        current_policy = emr_client.get_managed_scaling_policy(self.emr_id)
        current_max_capacity_units = current_policy['ManagedScalingPolicy']['ComputeLimits']['MaximumCapacityUnits']
        current_max_core_units = current_policy['ManagedScalingPolicy']['ComputeLimits']['MaximumCoreCapacityUnits']

        # 如果 apps_pending 为 0
        if apps_pending == 0:
            Utils.logger.info("No pending applications, setting minimum capacity.")
            current_policy['ManagedScalingPolicy']['ComputeLimits']['MaximumCapacityUnits'] = self.minimumUnits
            current_policy['ManagedScalingPolicy']['ComputeLimits']['MaximumOnDemandCapacityUnits'] = self.maximumOnDemandInstancesNumValue

        # 如果 apps_pending 不为 0
        else:
            # 更新 MaximumOnDemandCapacityUnits
            current_policy['ManagedScalingPolicy']['ComputeLimits']['MaximumOnDemandCapacityUnits'] = self.maximumOnDemandInstancesNumValue

            # 计算新的 MaximumCapacityUnits
            new_max_capacity_units = max(self.minimumUnits, current_max_capacity_units - int((pending_virtual_cores / apps_pending) * self.scaleInFactor))

            # 更新 MaximumCapacityUnits
            current_policy['ManagedScalingPolicy']['ComputeLimits']['MaximumCapacityUnits'] = new_max_capacity_units

        # 应用新策略
        emr_client.put_managed_scaling_policy(self.emr_id, current_policy['ManagedScalingPolicy'])

        # 记录新策略到 SQLite
        sanitized_table_name = self.sanitize_table_name(f"{self.emr_id}_ms_invoke_log")
        conn = sqlite3.connect(f"{sanitized_table_name}.db")
        c = conn.cursor()
        c.execute(f"CREATE TABLE IF NOT EXISTS {sanitized_table_name} (Timestamp INTEGER PRIMARY KEY, MaximumCapacityUnits INTEGER)")
        c.execute(f"INSERT INTO {sanitized_table_name} (Timestamp, MaximumCapacityUnits) VALUES (?, ?)", (int(datetime.now().timestamp()), current_policy['ManagedScalingPolicy']['ComputeLimits']['MaximumCapacityUnits']))
        conn.commit()
        conn.close()

        Utils.logger.info(f"New policy applied: {current_policy['ManagedScalingPolicy']}")

        # 修改 Instance Fleets
        instance_fleets = emr_client.list_instance_fleets(ClusterId=self.emr_id)['InstanceFleets']
        for fleet in instance_fleets:
            if fleet['InstanceFleetType'] == 'TASK':
                emr_client.modify_instance_fleet(
                    ClusterId=self.emr_id,
                    InstanceFleet={
                        'InstanceFleetId': fleet['Id'],
                        'TargetOnDemandCapacity': 0,
                        'TargetSpotCapacity': current_policy['ManagedScalingPolicy']['ComputeLimits']['MaximumCapacityUnits'] - current_max_core_units
                    }
                )
                Utils.logger.info(f"Modified Instance Fleet {fleet['Id']} to have 0 On-Demand instances and {current_policy['ManagedScalingPolicy']['ComputeLimits']['MaximumCapacityUnits'] - current_max_core_units} Spot instances.")