import asyncio
import time
import aiohttp

from engine import TasksEngine, FetchTask

"""
Внешний шедулер запускает задачу на обновление данных по клиенту.
евенты берем постранично, чтобы выяснить, следующую страницу.
Создаем Задачу next_page на закачку,ставим ее в очередь, 
Далее в цикле пока есть next_page:
    ждем пока эвенты в next_page загрузятся.
    если эвенты не загрузились,
        пробуем три раза и, если не получается, выходим из закачек совсем. до следующего старта шедулера.
    current_page = next_page, если есть следующая страница евентов,создаем задачу next_page, 
    добавляем в очередь на закачку (приоритет 0), чтобы после разбора current_page следующая next_page была уже готова.
    Если следующей страницы нет, next_page = None
    разбираем current_page с эвентами: фасуем/тасуем данные, обсчитываем,...
    Если next_page == None или обработано страниц > 50:
        сохраняем порцию данных,. 
        фиксируем последний обработанный эвент (дата)
        выходим.
"""


class Loader:
    def __init__(
            self, engine: TasksEngine, refer: str, token: str, last_date_event_loaded: int = 0,
            limit: int = 250
    ):
        self.refer = refer
        self.token = token
        self.last_date_event_loaded = last_date_event_loaded
        self.limit = limit
        self.engine = engine

    async def fetch_data_from(self):
        print("Loader.fetch_data_from start")
        headers = {"Authorization": "Bearer {token}".format(token=self.token)}
        # page: int = 1
        pages2fetch = 50
        async with aiohttp.ClientSession(headers=headers) as session:
            for page in range(pages2fetch):
                name = "e" + str(page + 1)
                url = "https://{ref}/api/v4/events?limit={limit}&page={page}" \
                      "&filter[created_at][from]={created_from}" \
                      "&order[created_at]=asc" \
                    .format(ref=self.refer, limit=self.limit, page=page + 1, created_from=self.last_date_event_loaded)

                # print("URL:", url)
                # print("Headers:", headers)
                task = FetchTask(priority=pages2fetch - page, session=session, name=name, url=url)
                # task = FetchTask(priority=1, session=session, name=name, url=url)
                self.engine.add_task(task=task)

            print("ждем готовности задач")
            # await asyncio.sleep(2)
            await self.engine.pqueue.join()

            # print("\nfinished:", self.engine.finished)
            # print("\nret", self.engine.pop_finished_task(name))
            # print("\nfinished:", self.engine.finished)
            # print("qsize:", len(self.engine.finished))
            # self.engine.finished.clear()
            # print("qsize:", len(self.engine.finished))

            print("\nLoader.fetch_data_from finished")


async def test():
    print("start test")
    fetcher = TasksEngine()  # глобальный объект должен быть объявлен на все приложение
    await fetcher.start()
    loader = Loader(
        fetcher, refer="",
        token=""
    )
    ts = time.strftime('%X')
    await loader.fetch_data_from()
    await fetcher.stop()
    print("Start time:", ts)
    print("Finish time:", time.strftime('%X'))
    print("exit")

if __name__ == "__main__":
    asyncio.run(test())
