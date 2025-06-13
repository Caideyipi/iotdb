import concurrent.futures
import threading
from multiprocessing import Queue

from ainode.core.log import Logger

logger = Logger()


class ParallelTaskExecutor(threading.Thread):
    """
    A thread-based executor for running tasks in parallel using a thread pool.

    Attributes:
        executor (concurrent.futures.ThreadPoolExecutor): The thread pool executor, where the number of executing tasks is no greater than max_workers.
        task_queue (Queue): A queue to hold tasks to be executed.
        futures (list): A list to keep track of futures for submitted tasks.
    """

    def __init__(self, max_workers=5):
        super().__init__()
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self.task_queue = Queue()
        self.futures = []

    def submit_task(self, func, *args, **kwargs):
        self.task_queue.put((func, args, kwargs))
        logger.info(
            f"Task submitted: {func.__name__} with args {args} and kwargs {kwargs}"
        )

    def run(self):
        self._process_tasks()

    def _process_tasks(self):
        while True:
            func, args, kwargs = self.task_queue.get()
            try:
                future = self.executor.submit(func, *args, **kwargs)
                self.futures.append(future)
            except Exception as e:
                logger.error(f"Task failed with exception: {e}")

    def wait_for_all(self):
        for future in concurrent.futures.as_completed(self.futures):
            try:
                result = future.result()
                logger.info(f"Task completed with result: {result}")
            except Exception as e:
                logger.error(f"Task failed with exception: {e}")

    def shutdown(self):
        self.executor.shutdown()
        logger.info("Executor shut down.")
