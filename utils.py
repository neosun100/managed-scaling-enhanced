from functools import wraps

class Utils:
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
                logger.error(f"An error occurred in {func.__name__}: {e}")
                raise
        return wrapper
