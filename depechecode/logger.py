#! /usr/bin/python3

# logger.py
#
# Project name: DepecheCode
# Author: Hugo Juhel
#
# description:
"""
Retrieve the package level logger. Configure the handler
"""

#############################################################################
#                                 Packages                                  #
#############################################################################

# System packages
import logging
import os

#############################################################################
#                                 helpers                                   #
#############################################################################


_LOGGERS = dict()


def get_module_logger(log_name="DepecheCode") -> logging.Logger:
    """
    Retrieve the current formatted logger
    """

    try:
        logger = _LOGGERS[log_name]
    except KeyError:
        logger = logging.getLogger(log_name)
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s [%(name)-12s] : %(levelname)-1s : %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(os.environ.get("LOG_LEVEL", "DEBUG").upper())
        logger.propagate = False
        _LOGGERS[log_name] = logger

    return logger


class MixinLogable:
    def __init__(self, logger_name: str = "DepecheCode", *args, **kwargs):
        self._logger = get_module_logger(logger_name)

    def warn(self, msg):
        self._logger.warn(msg)

    def info(self, msg):
        self._logger.info(msg)

    def debug(self, msg):
        self._logger.debug(msg)
