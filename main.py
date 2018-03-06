import datetime
import requests
import threading
import pytz

from datasources.models import DataSource
from importsessions.models import UpdateSession

from DatetimeUtils import DatetimeUtils
from MyLogger import MyLogger

logger = MyLogger('reimport_script.log', MyLogger.INFO)


class UpdateSessionJobsManager:
    LOG_TAG = 'UpdateSessionJobsManager'
    MAX_ATTEMPTS_PER_DAY = 3
    _dates_attempts = {}
    _queue = []
    _pool_size = 2
    _jobs_running = 0
    total_imports  = 0
    _finished_relaunch = None

    def __init__(self, pool_size=2):
        self._pool_size = pool_size

    def enqueue_job(self, date_from, date_to):
        logger.debug(msg='enqueue_job date_from=' + str(date_from) + ' date_to=' + str(date_to), tag=self.LOG_TAG)

        if str(date_from) not in self._dates_attempts:
            self._dates_attempts[str(date_from)] = 0

        if self._dates_attempts[str(date_from)] < UpdateSessionJobsManager.MAX_ATTEMPTS_PER_DAY:
            job = UpdateSessionJob(date_from, date_to)
            job.set_finished_callback(self._finished_relaunch)
            job.set_max_attempts_exceeded_callback(self.on_job_timeout_attempts_exceeded)
            self._queue.insert(0, job) # will be the next to be processed
            self._dates_attempts[str(date_from)] += 1
        else:
            logger.debug(msg='Will not enqueue again date ' + str(date_from) + ' because max nr of attempts (' +
                  str(UpdateSessionJobsManager.MAX_ATTEMPTS_PER_DAY) + ') was reached', tag=self.LOG_TAG)
            self._dates_attempts.pop(str(date_from))  # removed to free memory; date will not be tried again

        if self._can_start_new_job():
            logger.debug(msg='Will process next job', tag=self.LOG_TAG)
            self._start_next_job()

    def on_job_finished(self, date_from, date_to):
        self._finished_relaunch(date_from, date_to)
        self._jobs_running -= 1
        self.total_imports += 1

        logger.info(msg="Total successful reimports until now: " + str(self.total_imports), tag=self.LOG_TAG)

        if self._can_start_new_job():
            logger.debug(msg='Will process next job')
            self._start_next_job()

    def on_job_timeout_attempts_exceeded(self, date_from, date_to):
        logger.info(msg='Will not try to process again date ' + str(date_from), tag=self.LOG_TAG)
        self._jobs_running -= 1

        logger.info(msg="Total successful reimports until now: " + str(self.total_imports), tag=self.LOG_TAG)

        if self._can_start_new_job():
            logger.debug(msg='Will process next job', tag=self.LOG_TAG)
            self._start_next_job()

    def _start_next_job(self):
        if len(self._queue) > 0:
            job = self._queue.pop()
            job.start()
            self._jobs_running += 1
            logger.debug(tag=self.LOG_TAG, msg='UpdateSessionJobsManager.queue has ' + str(len(self._queue)) +
                         ' more pending jobs')

    def _can_start_new_job(self):
        return self._jobs_running < self._pool_size and len(self._queue) > 0

    def set_relaunch_function(self, finished_relaunch_function):
        self._finished_relaunch = finished_relaunch_function

    def has_pending_items(self):
        return self._jobs_running > 0 or len(self._queue) > 0

class UpdateSessionJob:
    LOG_TAG = 'UpdateSessionJob'
    MAX_ATTEMPTS = 3
    JOB_TIME_LIMIT = 3 * 60 * 60  # 3 hs
    CHECK_TIME = 5 * 60  # 5 min
    _date_from = None
    _date_to = None
    _update_job = None
    _max_time_job_timer = None
    _check_timer = None
    _finished_callback = None
    _timeout_max_attempts_callback = None
    _has_to_restart = None
    _attempts = 0
    _running = False

    def __init__(self, date_from, date_to):
        self._date_from = date_from
        self._date_to = date_to

    def start(self):
        logger.debug('UpdateSessionJob.start')
        self._running = True
        ds = DataSource.objects.filter(name="DNM - OSTOLBDA")[0]
        self._update_job = UpdateSession.objects.create(name="script_reimport" + str(self._date_from),
                                                        from_date=datetime.datetime(self._date_from.year,
                                                                                    self._date_from.month,
                                                                                    self._date_from.day,
                                                                                    3,
                                                                                    0,
                                                                                    0,
                                                                                    tzinfo=pytz.UTC),
                                                        to_date=datetime.datetime(self._date_to.year,
                                                                                  self._date_to.month,
                                                                                  self._date_to.day,
                                                                                  2,
                                                                                  59,
                                                                                  59,
                                                                                  tzinfo=pytz.UTC),
                                                        source=ds)
        self._launch()
        self._attempts = 1

    def _launch(self):
        self._update_job_reset()
        self._reset_timer_check()
        self._reset_job_time_limit_timer()

    def _update_job_reset(self):
        self._update_job.status = UpdateSession.STATUS_READY
        self._update_job.save()

    def _on_check_timeout(self):
        logger.debug(msg='check_running: ' + str(self._update_job.id), tag=self.LOG_TAG)
        self._update_job.refresh_from_db()
        if self._running:
            if self._update_job.status == UpdateSession.STATUS_FINISHED:
                logger.info(msg='UpdateSessionJob with id=' + str(self._update_job.id) + ' finished', tag=self.LOG_TAG)
                self.notify_job_finished(self._date_from, self._date_to)
            else:
                logger.debug(msg='UpdateSessionJob with id=' + str(self._update_job.id) + ' not finished. Continue.', tag=self.LOG_TAG)
                self._reset_timer_check()
        else:
            self._cancel_timers()

    def set_finished_callback(self, callback_function):
        if callback_function:
            self._finished_callback = callback_function
        else:
            logger.error(msg='Callback for job is null!', tag=self.LOG_TAG)

    def set_max_attempts_exceeded_callback(self, callback_function):
        self._timeout_max_attempts_callback = callback_function

    def notify_job_finished(self, date_from, date_to):
        logger.info(msg='Job processing from ' + str(date_from) + ' to ' + str(date_to) + ' finished.', tag=self.LOG_TAG)
        self._running = False
        self._cancel_timers()

        if self._finished_callback is not None:
            self._finished_callback(date_from, date_to)

    def notify_job_max_attempts_exceeded(self, date_from, date_to):
        logger.info(msg='notify_job_max_attempts_exceeded ' + str(date_from), tag=self.LOG_TAG)
        self._update_job.refresh_from_db()

        if self._timeout_max_attempts_callback:
            self._timeout_max_attempts_callback(date_from, date_to)

    def _cancel_timers(self):
        if self._check_timer:
            self._check_timer.cancel()
            self._check_timer = None
        if self._max_time_job_timer:
            self._max_time_job_timer.cancel()
            self._max_time_job_timer = None

    def _reset_timer_check(self):
        if self._check_timer:
            self._check_timer.cancel()

        if self._running:
            self._check_timer = threading.Timer(UpdateSessionJob.CHECK_TIME, self._on_check_timeout).start()

    def set_restart_check_function(self, check_function):
        self._has_to_restart = check_function

    def _on_task_timeout(self):
        logger.debug(tag=self.LOG_TAG, msg='_relaunch_task_timeout ' + str(self._update_job.id))

        self._update_job.refresh_from_db()

        if self._running:
            if self._update_job.status == UpdateSession.STATUS_FINISHED:
                logger.info(msg='UpdateSessionJob with id=' + str(self._update_job.id) + ' finished', tag=self.LOG_TAG)
                self.notify_job_finished(self._date_from, self._date_to)
            else:
                if self._attempts < UpdateSessionJob.MAX_ATTEMPTS:
                    logger.debug(msg='UpdateSessionJob with id=' + str(self._update_job.id) + ' will be relaunched due to timeout')
                    self._launch()
                    self._attempts += 1
                else:
                    logger.info(msg='UpdateSessionJob with id=' + str(self._update_job.id) + 'reached max nr of attempts (' +
                                 str(UpdateSessionJob.MAX_ATTEMPTS) + ') caused by time outs', tag=self.LOG_TAG)
                    self._running = False
                    self._cancel_timers()
                    self.notify_job_max_attempts_exceeded(self._date_from, self._date_to)

    def _reset_job_time_limit_timer(self):
        if self._max_time_job_timer:
            self._max_time_job_timer.cancel()

        if self._running:
            self._max_time_job_timer = threading.Timer(UpdateSessionJob.JOB_TIME_LIMIT,
                                                       self._on_task_timeout).start()


reimport_jobs_pool = UpdateSessionJobsManager()

def count_difference(date_from):
    elastic_count = count_data_elastic(DatetimeUtils.date_to_elastic_str(date_from))
    oracle_count = count_data_oracle(DatetimeUtils.date_to_datetime(date_from))

    logger.debug(msg=str(date_from) + ' oracle_count: ' + str(oracle_count))
    logger.debug(msg=str(date_from) + ' elastic_count: ' + str(elastic_count))

    return elastic_count - oracle_count

def count_entries_in_databases(date_from, date_to):
    logger.debug(msg='launch_count_job ' + str(date_from) + ' ' + str(date_to))

    diff = count_difference(date_from)

    if diff < 0:
        logger.info(msg='Must reimport data for date ' + str(date_from))
        reimport_jobs_pool.enqueue_job(date_from, date_to)
    elif diff > 0:
        logger.info(msg='Will not reimport data for date ' + str(date_from) + ' but elastic count was greater')
    else:
        logger.info(msg='Everything ok on date ' + str(date_from))

def count_data_oracle(datetime_start):
    con = DataSource.objects.get(name='DNM - OSTOLBDA').connector.connect()

    datetime_end = datetime_start + datetime.timedelta(days=1, seconds=-1)
    datetime_end_str = DatetimeUtils.datetime_to_oracle_str(datetime_end)
    datetime_start_str = DatetimeUtils.datetime_to_oracle_str(datetime_start)

    logger.debug(msg='count_data_oracle from ' + datetime_start_str + ' to ' + datetime_end_str)

    bind = {'fecha_desde': datetime_start_str, 'fecha_hasta': datetime_end_str}
    query = "SELECT count(rid_idtran_ol_sicam) FROM bda.transitos WHERE fecha_insert BETWEEN TO_DATE(:fecha_desde, 'DD/MM/YYYY HH24:MI:SS') AND TO_DATE(:fecha_hasta, 'DD/MM/YYYY HH24:MI:SS')"

    data = con.execute(query, bind).fetchall()
    logger.debug(data)
    return data[0][0] # [(result)]

def count_data_elastic(date_str):
    logger.debug('count_data_elastic ' + date_str)
    count = -1
    url = 'http://sma-prod-elasticsearch.apps.migraciones.cloud/crossing_index/_search?scroll=1m'
    json_str = '{"from":0,"query":{"constant_score":{"filter":{"bool":{"must":[{"term":{"_source_last_update":"'+ date_str +'"}},{"query_string":{"query":"(source:SICAM)"}}]}}}}}'

    response = requests.post(url, data=json_str, headers={"content-type":"application/json", "Authorization": "Basic YWRtaW46Y2hhbmdlbWU="})
    logger.debug(msg='elastic response code = ' + str(response.status_code))

    if requests.codes.ok == response.status_code:
        results = response.json()
        if results['hits']:
            count = results['hits']['total']

    return count


def main(args):
    if len(args) < 3:
        print("Usage example: 28-02-2018 01-03-2018")
        return

    logger.info(msg="PROCESS STARTED")

    date_start = DatetimeUtils.string_to_date(args[1])
    start_datetime = DatetimeUtils.date_to_datetime(date_start)
    final_timedate = DatetimeUtils.date_to_datetime(DatetimeUtils.string_to_date(args[2]))

    start_date = start_datetime.date()
    end_date = final_timedate.date()
    day_count = (end_date - start_date).days + 1
    date_from = start_date
    all_dates_to_process = [d for d in (start_date + datetime.timedelta(n + 1) for n in range(day_count)) if d <= end_date]

    reimport_jobs_pool.set_relaunch_function(count_entries_in_databases)

    for date_to in all_dates_to_process:
        logger.info(msg='Will check datetime from: ' + str(date_from) + ' to ' + str(date_to))
        count_entries_in_databases(date_from, date_to)
        date_from = date_to

    logger.info(msg="Total dates: " + str(len(all_dates_to_process)))


if __name__ == '__main__':
    main(['', '07-11-2017', '12-11-2017'])
