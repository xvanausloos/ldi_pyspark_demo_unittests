from common_ldi.ldi_logger import LdiLogger
from pyspark.sql import DataFrame

def main() -> None:
    logger = LdiLogger.getlogger("ldi_python")
    logger.setLevel("DEBUG")
    application = Application()
    ret = application.add_one(2)
    logger.info(f"Result : {ret}")
    logger.info("*** end ***")


class Application:
    def __init__(self) -> None:
        self._logger = LdiLogger.getlogger("ldi_python")

    def add_one(self, val:float):
        return val + 1

if __name__ == '__main__':
    main()
