from UpdateSessionJob import UpdateSessionJob
import logging

class UpdateSessionJobsPool:
    _queue = []
    _pool_size = 2
    _jobs_running = 0
    total_imports  = 0
    _finished_relaunch = None

    def __init__(self, pool_size=2):
        self._pool_size = pool_size

    def enqueue_job(self, date_from, date_to):
        print('Enqueued job date_from=' + str(date_from) + ' date_to=' + str(date_to))
        job = UpdateSessionJob(date_from, date_to)
        job.set_finished_callback(self._finished_relaunch)
        self._queue.append(job)

        if self._can_start_new_job():
            print('Can start new job!')
            self._start_next_job()

    def _on_job_finished(self, date_from, date_to):
        self._finished_relaunch(date_from, date_to)

    def _start_next_job(self):
        job = self._queue.pop()
        job.start()
        self._jobs_running += 1
        self.total_imports += 1

    def _can_start_new_job(self):
        return self._jobs_running < self._pool_size and len(self._queue) > 0

    def set_relaunch_function(self, finished_relaunch_function):
        self._finished_relaunch = finished_relaunch_function

    def has_pending_items(self):
        return self._jobs_running > 0 or len(self._queue) > 0