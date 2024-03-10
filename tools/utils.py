from functools import wraps
from loguru import logger

class Utils:
    """
    一个工具类，提供日志记录和异常处理装饰器功能。
    """
    # 将logger定义为Utils类的一个静态属性，以便于在类的静态方法中使用
    logger = logger

    @staticmethod
    def exception_handler(func):
        """
        异常处理装饰器，用于捕获和记录函数中的异常。
        :param func: 被装饰的函数
        :return: 装饰器包装后的函数
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # 使用Utils类静态属性logger来记录异常信息
                Utils.logger.error(f"An error occurred in {func.__name__}: {e}")
                raise e
        return wrapper