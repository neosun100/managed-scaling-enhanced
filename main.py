from managed_scaling_enhanced import ManagedScalingEnhanced
from tools.utils import Utils

# 配置loguru的logger
Utils.logger.add("managed_scaling_enhanced.log",
                 format="{time} {level} {message}", level="DEBUG")

managed_scaling_enhanced_client = ManagedScalingEnhanced()

# 示例调用，这里需要替换为实际参数
if __name__ == '__main__':

    scaleStatus = managed_scaling_enhanced_client.determine_scale_status(
        emr_id='j-1F74M1P9SC57B', prefix="managedScalingEnhanced"
    )
    Utils.logger.info(f"Scale Status: {scaleStatus}")
