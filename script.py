import json
import openpyxl
import requests
import sqlite3
import time
import sys
import threading
import logging


class MyThread(threading.Thread):

    def __init__(self, key, url, logger):
        threading.Thread.__init__(self)
        self.key = key
        self.url = url
        self.logger = logger

    def run(self):
        logger.debug('Calling get_pull')
        get_pull(self.url, self.key, logger)

def get_logger():

    logger = logging.getLogger("requests_log")
    logger.setLevel(logging.DEBUG)
#    fherror = logging.FileHandler("errors.log")  # это будет файл в формате json
    fhdebug = logging.FileHandler("debug.log")
#    fmterror = '%(message)s'
    fmtdebug = '%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'
    formatterdebug = logging.Formatter(fmtdebug)
#    formattererror = logging.Formatter(fmterror)
    fhdebug.setFormatter(formatterdebug)
#    fherror.setFormatter(formattererror)
#    logger.addHandler(formattererror)
    logger.addHandler(fhdebug)
    return logger


def get_pull(url, key, logger):

    logger.debug('get_pull function executing')
    cont_length = None
    st = time.time()
    try:
        resp = requests.get(url)
    except Exception as error:
        logger.error(error)
    else:

        if resp.status_code == 200:
            cont_length = sys.getsizeof(resp.text)
        data = (st, url, key, round((time.time() - st) * 1000), resp.status_code, cont_length)
        results.append(data)
        logger.debug('get_pull function ended with: {}'.format(data))


if __name__ == '__main__':

    logger = get_logger()
    wb = openpyxl.load_workbook(filename='raw_data.xlsx')
    sheet = wb['Лист1']
    urls = {sheet['B%s' % i].value: sheet['A%s' % i].value for i in range(2, sheet.max_row + 1) if
            sheet['C%s' % i].value}
    results = []
    for key in urls:
        while True:
            if threading.active_count() <= 4:
                thread = MyThread(key, urls[key], logger)
                thread.setName(urls[key])
                thread.start()
                break

    while True:
        if threading.active_count() == 1:
            conn = sqlite3.connect("urls.db")
            cursor = conn.cursor()

            cursor.execute("""CREATE TABLE IF NOT EXISTS requests
                              (TS real, URL text, LABEL text,
                               RESPONSE_TIME real, STATUS_CODE integer, CONTENT_LENGTH integer)
                           """)
            cursor.executemany("INSERT INTO requests VALUES (?,?,?,?,?,?)", results)
            conn.commit()
            break


