from requests.exceptions import Timeout as RequestsTimeout, HTTPError, TooManyRedirects

from executors.bounded_executor import BoundedExecutor
from executors.fail_safe_executor import FailSafeExecutor
from utils.progress_logger import ProgressLogger
from utils.iter_utils import dynamic_batch_iterator

RETRY_EXCEPTIONS = (ConnectionError, HTTPError, RequestsTimeout, TooManyRedirects, OSError)


# Executes the given work in batches, reducing the batch size exponentially in case of errors.
class BatchWorkExecutor:
    def __init__(self, starting_batch_size, max_workers, retry_exceptions=RETRY_EXCEPTIONS):
        self.batch_size = starting_batch_size
        self.max_workers = max_workers
        # Using bounded executor prevents unlimited queue growth
        # and allows monitoring in-progress futures and failing fast in case of errors.
        self.executor = FailSafeExecutor(BoundedExecutor(1, self.max_workers))
        self.retry_exceptions = retry_exceptions
        self.progress_logger = ProgressLogger()

    def execute(self, work_iterable, work_handler, total_items=None):
        self.progress_logger.start(total_items=total_items)
        for batch in dynamic_batch_iterator(work_iterable, lambda: self.batch_size):
            self.executor.submit(self._fail_safe_execute, work_handler, batch)

    # Check race conditions
    def _fail_safe_execute(self, work_handler, batch):
        try:
            work_handler(batch)
        except self.retry_exceptions:
            batch_size = self.batch_size
            # Reduce the batch size. Subsequent batches will be 2 times smaller
            if batch_size == len(batch) and batch_size > 1:
                self.batch_size = int(batch_size / 2)
            # For the failed batch try handling items one by one
            for item in batch:
                work_handler([item])
        self.progress_logger.track(len(batch))

    def shutdown(self):
        self.executor.shutdown()
        self.progress_logger.finish()
