from common.ldi_logger import LdiLogger


def main() -> None:
    logger = LdiLogger.getlogger("ldi_python")
    logger.setLevel("DEBUG")
    application = Application()
    ret = application.increment_by_two(x=2)
    logger.info(f"Result : {ret}")
    logger.info("*** end ***")


class Application:
    def __init__(self) -> None:
        self._logger = LdiLogger.getlogger("ldi_python")

    """Implementation of basic summation example"""
    def increment_by_two(self, x: int) -> int:
        """
        Increment the provided parameter by 2
        Args:
            x: integer parameter which will be summed by 2
        Returns: x + 2
        """
        return x + 2

if __name__ == '__main__':
    main()
