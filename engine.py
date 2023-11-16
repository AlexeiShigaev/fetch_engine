import asyncio
import json

import aiohttp
import time
from dataclasses import dataclass


@dataclass(order=True)
class FetchTask:
    priority: int = 0  # Приоритет выполнения из очереди PriorityQueue
    session: aiohttp.ClientSession = None
    url: str = ""
    name: str = ""  # TasksEngine имеет словарь выполненных задач, это ключ.

    async def fetch(self):
        start = time.time()

        async with self.session.get(self.url) as resp:
            try:
                event_resp_json = await resp.json()  # интересует json
                response = resp
            except aiohttp.ContentTypeError:
                print("\n\tОтвет пуст (FetchTask:{})".format(self.name))
            if not resp.status == 200:
                print("\n\tСтатус ответа {} (FetchTask:{})".format(resp.status, self.name))

            # with open(f"res/events/{self.name}.json", "w", encoding="utf-8") as req_f:
            #     req_f.write(json.dumps(event_resp_json, ensure_ascii=False))

        end = time.time()
        print("{page}({t:.2f})".format(page=self.name, t=(end - start)), end=", ")
        return response


class TasksEngine:
    """
    Используется для постановки задач в очередь на загрузку урл.
    Запросы выполняются с ограничением количества в секунду. sleep_time = 0.143 - Это 7 запросов/сек.
    В первую очередь берутся задачи с наименьшим значением приоритета FetchTask.priority
    """
    finished = {}

    def __init__(self, sleep_time: float = 0.143):
        self._sleep_time = sleep_time
        self._is_running = False
        self.pqueue = asyncio.PriorityQueue()
        self._run_task: asyncio.Task = None

    async def start(self, sleep_time: float = None):
        if self._is_running:
            print("Service already started. Stop first.")
            return
        self._is_running = True
        self._sleep_time = sleep_time or self._sleep_time
        self._run_task = asyncio.create_task(self.run())
        print("TasksEngine started.")

    async def _worker(self, task: FetchTask):
        try:
            self.finished[task.name] = await task.fetch()
        except Exception as ex:
            print("exception:", ex)
        self.pqueue.task_done()

    async def run(self):
        if not self._is_running:
            print("TasksEngine doesn't start yet. Use start().")
            return
        print("Run started.")
        while self._is_running:
            if self.pqueue.qsize():
                task = await self.pqueue.get()
                asyncio.create_task(self._worker(task))
            await asyncio.sleep(self._sleep_time)
        print("Run stopped.")

    def add_task(self, task):
        self.pqueue.put_nowait(task)

    def pop_finished_task(self, key):
        if key not in self.finished:
            return None
        return self.finished.pop(key)

    def clear_finished(self):
        self.finished.clear()

    async def stop(self):
        self._is_running = False
        await self.pqueue.join()
        print("PriorityQueue is empty.")
        if self._run_task is not None:
            while not self._run_task.done():
                await asyncio.sleep(0.15)
        print("TasksEngine stopped.")
