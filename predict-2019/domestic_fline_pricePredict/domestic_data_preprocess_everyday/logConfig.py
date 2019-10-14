# coding:utf-8
# -*- coding:utf-8 -*-

import logging
from logging import handlers
import config


def log():
    logFileName = config.logFileName
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt='[%(asctime)s] - %(levelname)s: %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    rotating_file = handlers.RotatingFileHandler(filename=logFileName,  mode='a', maxBytes=1024*1024*1024, backupCount=1)
    rotating_file.setLevel(logging.DEBUG)
    rotating_file.setFormatter(formatter)
    logger.addHandler(rotating_file)
    return logger

logger = log()