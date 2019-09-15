import logging
import sqlite3
import sys
import threading
import time
import traceback
import openpyxl
import requests
from pythonjsonlogger import jsonlogger


class MyThread(threading.Thread):

    def __init__(self, key, url, logger):
        threading.Thread.__init__(self)
        self.key = key
        self.url = url
        self.logger = logger

    def run(self):
        logger.debug('Calling get_pull')
        get_pull(self.url, self.key, logger)


class CustomJsonFormatter(jsonlogger.JsonFormatter):

    def add_fields(self, log_record, record, message_dict):
        super(CustomJsonFormatter, self).add_fields(log_record, record, message_dict)
        log_record['timestamp'] = time.time()
        log_record['url'] = log_record['message']
        del log_record['message']


def get_logger():
    logger = logging.getLogger(config.debug_path)
    logger.setLevel(logging.DEBUG)

    fhdebug = logging.FileHandler("debug.log")
    fmtdebug = '%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'
    formatterdebug = logging.Formatter(fmtdebug)
    fhdebug.setFormatter(formatterdebug)
    logger.addHandler(fhdebug)

    shdebug = logging.StreamHandler(sys.stdout)
    shdebug.setFormatter(formatterdebug)
    logger.addHandler(shdebug)

    fherror = logging.FileHandler(config.errors_path)
    fherror.setLevel(logging.ERROR)
    formattererror = CustomJsonFormatter('(timestamp) (url) (message)')
    fherror.setFormatter(formattererror)
    logger.addHandler(fherror)

    return logger


def get_pull(url, key, logger):
    logger.debug('get_pull function executing')
    cont_length = None
    st = time.time()
    try:
        resp = requests.get(url, timeout=config.req_timeout)
    except Exception as error:
        logger.error(url, extra={'error': {'exception_type': error.__class__.__name__,
                                           'exception_value': error.args,
                                           'stack_info': traceback.extract_tb(sys.exc_info()[2])}})
    else:

        if resp.status_code == 200:
            cont_length = sys.getsizeof(resp.text)
        data = (st, url, key, round((time.time() - st) * 1000), resp.status_code, cont_length)
        results.append(data)
        logger.debug('get_pull function ended with: {}'.format(data))


if __name__ == '__main__':

    assert len(sys.argv) > 1
    import config

    logger = get_logger()
    try:
        wb = openpyxl.load_workbook(filename=sys.argv[1])
    except openpyxl.utils.exceptions.InvalidFileException:
        logger.debug('Input file not found or not in xlsx format')
        exit(1)

    sheet = wb['Лист1']
    urls = {sheet['B%s' % i].value: sheet['A%s' % i].value for i in range(2, sheet.max_row + 1) if
            sheet['C%s' % i].value}
    results = []
    for key in urls:
        while True:
            if threading.active_count() <= config.threads_count:
                thread = MyThread(key, urls[key], logger)
                thread.setName(urls[key])
                thread.start()
                break

    while True:
        if threading.active_count() == 1:
            conn = sqlite3.connect(config.db_path)
            cursor = conn.cursor()

            cursor.execute("""CREATE TABLE IF NOT EXISTS requests
                              (TS real, URL text, LABEL text,
                               RESPONSE_TIME real, STATUS_CODE integer, CONTENT_LENGTH integer)
                           """)
            cursor.executemany("INSERT INTO requests VALUES (?,?,?,?,?,?)", results)
            conn.commit()
            break
