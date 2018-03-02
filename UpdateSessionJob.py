import logging
import threading
import datetime

from importsessions.models import UpdateSession
from datasource.models import DataSource

class UpdateSessionJob:
    TASK_TIME_LIMIT = 60 * 60 * 3  # 3 hs
    CHECK_TIME = 5 * 60  # 5 min
    _date_from = None
    _date_to = None
    _update_job = None
    _check_timer = None
    _finished_callback = None
    _has_to_restart = None

    def __init__(self, date_from, date_to):
        self._date_from = date_from
        self._date_to = date_to

    def start(self):
        logging.debug('UpdateSessionJob.start id=' + self._update_job.id)
        ds = DataSource.objects.filter(name="DNM - OSTOLBDA")[0]
        self._update_job = UpdateSession.objects.create(name="import_job_" + self._date_from, from_date=datetime.datetime(self._date_from),
                                                         to_date=datetime.datetime(self._date_to), source=ds)
        self._update_job_reset()

    def _update_job_reset(self):
        self._update_job.status = UpdateSession.STATUS_READY
        self._update_job.save()
        self._set_check_timer()

    def check_running(self):
        logging.debug('check_running: Will check if ' + self._update_job.id + " has finished")
        if self._update_job.status == UpdateSession.STATUS_RUNNING and self._job_elapsed_time() > UpdateSessionJob.TASK_TIME_LIMIT:
            logging.info('UpdateSessionJob with id=' + self._update_job.id + ' will be relaunched due to time out')
            self._update_job_reset()
        elif self._update_job.status == UpdateSession.STATUS_FINISHED:
            logging.info('UpdateSessionJob with id=' + self._update_job.id + ' finished')
            self.notify_job_finished(self._date_from, self._date_to)
        else:
            self._set_check_timer()

    def _job_elapsed_time(self):
        return -1

    def set_finished_callback(self, callback_function):
        self._finished_callback = callback_function

    def notify_job_finished(self):
        if self._finished_callback is not None:
            self._finished_callback()

    def _set_check_timer(self):
        self._check_timer = threading.Timer(UpdateSessionJob.CHECK_TIME, self.check_running).start()

    def set_restart_check_function(self, check_function):
        self._has_to_restart = check_function
