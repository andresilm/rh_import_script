import logging
import threading
import datetime
import pytz

from importsessions.models import UpdateSession
from datasources.models import DataSource

class UpdateSessionJob:
    JOB_TIME_LIMIT = 60 * 60 * 3  # 3 hs
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
        #ds = DataSource.objects.filter(name="DNM - OSTOLBDA")[0]
        #self._update_job = UpdateSession.objects.create(name="script_reimport" + str(self._date_from),
        #                                                from_date=datetime.datetime(self._date_from.year,
        #                                                                            self._date_from.month,
        #                                                                            self._date_from.day,
        #                                                                            3,
        #                                                                            0,
        #                                                                            0,
        #                                                                            tzinfo=pytz.UTC),
        #                                                to_date=datetime.datetime(self._date_to.year,
        #                                                                          self._date_to.month,
        #                                                                          self._date_to.day,
        #                                                                          2,
        #                                                                          59,
        #                                                                          59,
        #                                                                          tzinfo=pytz.UTC),
        #                                                source=ds)
        self._update_job_reset()

    def _update_job_reset(self):
        print('UpdateSessionJob.start id=' + self._update_job.id)
        #self._update_job.status = UpdateSession.STATUS_READY
        #self._update_job.save()
        #self.update_job.refresh_from_db()
        self._reset_timer_check()
        self._reset_job_time_limit_timer()

    def check_running(self):
        print('check_running: Will check if ' + str(self._update_job.id) + " has finished")

        if self._update_job.status == UpdateSession.STATUS_FINISHED:
            print('UpdateSessionJob with id=' + str(self._update_job.id) + ' finished')
            self.notify_job_finished(self._date_from, self._date_to)
        else:
            self._reset_timer_check()

    def set_finished_callback(self, callback_function):
        if callback_function:
            self._finished_callback = callback_function
        else:
            print('Callback for job is null!')

    def notify_job_finished(self, date_from, date_to):
        if self._finished_callback is not None:
            self._finished_callback(date_from, date_to)

    def _reset_timer_check(self):
        self._check_timer = threading.Timer(UpdateSessionJob.CHECK_TIME, self.check_running).start()

    def set_restart_check_function(self, check_function):
        self._has_to_restart = check_function

    def _relaunch_task_timout(self):
        print('UpdateSessionJob with id=' + str(self._update_job.id) + ' will be relaunched due to time out')
        self._update_job_reset()

    def _reset_job_time_limit_timer(self):
        self._max_time_job_timer = threading.Timer(UpdateSessionJob.JOB_TIME_LIMIT, self._relaunch_task_timout).start()
