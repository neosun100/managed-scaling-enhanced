import boto3
import aioboto3
import asyncio
from .utils import Utils  # 使用相对导入从同一包内导入Utils类


class AWSSSMClient:
    """
    一个专门用于与AWS SSM服务交互的类。
    """

    def __init__(self):
        self.client = boto3.client('ssm')

    @Utils.exception_handler
    def get_parameters_from_parameter_store(self, name):
        """
        从AWS Parameter Store获取参数值。
        :param name: 参数的名称
        :return: 参数的值
        """
        Utils.logger.info(
            f"Getting parameter '{name}' from Parameter Store...")
        parameter = self.client.get_parameter(
            Name=name, WithDecryption=True)
        return parameter['Parameter']['Value']

    @Utils.exception_handler
    async def aioget_parameter_from_parameter_store(self, name):
        """
        异步从AWS Parameter Store获取单个参数值。
        :param name: 参数的名称
        :return: 参数的值
        """
        Utils.logger.info(
            f"Getting parameter '{name}' from Parameter Store asynchronously...")
        session = aioboto3.Session()
        async with session.client('ssm') as client:
            parameter = await client.get_parameter(Name=name, WithDecryption=True)
            return parameter['Parameter']['Value']

    @Utils.exception_handler
    async def aioget_parameters_from_parameter_store(self, names):
        """
        异步并行从AWS Parameter Store获取多个参数值。
        :param names: 参数名称的列表
        :return: 参数值的列表
        """
        Utils.logger.info(
            f"Getting parameters from Parameter Store asynchronously: {', '.join(names)}")
        tasks = [self.aioget_parameter_from_parameter_store(
            name) for name in names]
        return await asyncio.gather(*tasks)

    @Utils.exception_handler
    def write_parameters_to_parameter_store(self, parameters):
        """
        将参数写入AWS Parameter Store。
        :param parameters: 字典，包含参数名和值
        """
        for parameter, value in parameters.items():
            full_parameter_name = f"/{parameter}"
            Utils.logger.info(
                f"Writing parameter '{full_parameter_name}' to Parameter Store...")
            response = self.client.put_parameter(
                Name=full_parameter_name,
                Value=str(value),
                Type='String',
                Overwrite=True
            )
            Utils.logger.info(
                f"Parameter '{full_parameter_name}' written: {response}")
