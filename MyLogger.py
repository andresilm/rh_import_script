import datetime


class MyLogger:
    DEBUG = 2
    INFO = 1
    ERROR = 0
    _level = DEBUG
    _sep = ' '

    def __init__(self, filename='my_logger.log', level=ERROR):
        self._level = level
        self._output_file = open(filename, 'a')
        self._output_file.write('New MyLogger entry starts @ ' + str(datetime.datetime.now()))

    def debug(self, msg='', tag='root'):
        if self._level >= MyLogger.DEBUG:
            self._log('DEBUG', msg, tag)

    def info(self, msg='', tag='root'):
        if self._level >= MyLogger.INFO:
            self._log('INFO', msg, tag)

    def error(self, msg='', tag='root'):
        if self._level >= MyLogger.ERROR:
            self._log('ERROR', msg, tag)

    def _log(self, level, msg, tag='root'):
        self._output_file.write(str(datetime.datetime.now()) + self._sep + level + self._sep + tag + self._sep + str(msg) + '\n')
        self._output_file.flush()


