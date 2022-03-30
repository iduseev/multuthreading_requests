#!/bin/python

import threading
from concurrent.futures import ThreadPoolExecutor
from typing import List, Tuple

import requests


class Multi:
    def __init__(self) -> None:
        self.thread_pool_size = 128
        self.entities = []

    def run(self, tasks: List[Tuple]):
        """Multithreading

        tasks is a list of requests by hour, for example
        """
        batch_count = len(tasks) // self.thread_pool_size
        if batch_count * self.thread_pool_size < len(tasks):
            batch_count += 1

        print(f"Batches to process: {batch_count}")
        for bidx in range(batch_count):
            start_index = bidx * self.thread_pool_size
            end_index = min((bidx + 1) * self.thread_pool_size, len(tasks))

            print(f"Processing batch {bidx}. Starting index: {start_index}. Ending index: {end_index}.")

            with ThreadPoolExecutor(max_workers=self.thread_pool_size) as executor:
                for i in range(start_index, end_index):
                    data_1, data_2, data_3 = tasks[i]
                    executor.submit(
                        self.execute_task,
                        data_1, data_2, data_3,
                    )

    def execute_task(self, arg1, arg2, arg3):
        thread_local = threading.local()
        def _get_thread_session() -> requests.Session:
            if not hasattr(thread_local, "session"):
                setattr(thread_local, "session", requests.Session())
            return thread_local.session

        # arg1 is a list of things to query, for example. Maybe it's just a one request
        with session.get("https://www.api.com/api", headers={"some": "headers"}) as resp:
            data = resp.json()

        entities = data["entities"]  # List of entities ?
        self.entities.extend(entities)
