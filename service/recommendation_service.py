import logging
from settings import (
    PERSONAL_RECS_PATH,
    DEFAULT_RECS_PATH,
    EVENTS_SERVICE_URL,
    RECS_ONLINE_SERVICE_URL
)

from fastapi import FastAPI
from contextlib import asynccontextmanager

import pandas as pd
import requests

class Recommendations:

    def __init__(self):

        self._recs = {"personal": None, "default": None}
        self._stats = {
            "request_personal_count": 0,
            "request_default_count": 0,
        }

    def load(self, type, path, **kwargs):
        """
        Загружает рекомендации из файла
        """

        logger.info(f"Загрузка рекомендаций, type: {type}")
        self._recs[type] = pd.read_parquet(path, **kwargs)
        if type == "personal":
            self._recs[type] = self._recs[type].set_index("user_id")
        logger.info(f"Загружено")

    def get(self, user_id: int, k: int=100):
        """
        Возвращает список рекомендаций для пользователя
        """
        try:
            recs = self._recs["personal"].loc[user_id]
            recs = recs["track_id"].to_list()[:k]
            self._stats["request_personal_count"] += 1
            logger.info(f"User: {user_id} запрос персональных рекомендаций")
        except KeyError:
            recs = self._recs["default"]
            recs = recs["track_id"].to_list()[:k]
            self._stats["request_default_count"] += 1
            logger.info(f"User: {user_id} запрос default рекомендаций")
        except:
            logger.error("Рекомендаций не найдено")
            recs = []

        return recs

    def stats(self):

        logger.info("Stats for recommendations")
        for name, value in self._stats.items():
            logger.info(f"{name:<30} {value} ")

logger = logging.getLogger("uvicorn.error")
rec_store = Recommendations() 

@asynccontextmanager
async def lifespan(app: FastAPI):
    # код ниже (до yield) выполнится только один раз при запуске сервиса
    logger.info("Запуск")

    rec_store.load(
        "personal",
        PERSONAL_RECS_PATH,
        columns=["user_id", "track_id", "rank"],
    )
    rec_store.load(
        "default",
        DEFAULT_RECS_PATH,
        columns=["track_id", "tracks_rating"],
    )

    yield
    # этот код выполнится только один раз при остановке сервиса
    logger.info("Остановка")
    logger.info(rec_store.stats())

def dedup_ids(ids):
    """
    Дедублицирует список идентификаторов, оставляя только первое вхождение
    """
    seen = set()
    ids = [id for id in ids if not (id in seen or seen.add(id))]

    return ids
    
# создаём приложение FastAPI
app = FastAPI(title="recommendations", lifespan=lifespan)


@app.post("/recommendations")
async def recommendations(user_id: int, k: int = 100):
    """
    Возвращает список рекомендаций длиной k для пользователя user_id
    """

    recs_offline = await recommendations_offline(user_id, k)
    recs_online = await recommendations_online(user_id, k)

    if recs_online["recs"] == []:
        return {"recs": recs_offline["recs"]}

    recs_offline = recs_offline["recs"]
    recs_online = recs_online["recs"]

    recs_blended = []

    min_length = min(len(recs_offline), len(recs_online))
    # чередуем элементы из списков, пока позволяет минимальная длина
    for i in range(min_length):
        recs_blended.append(recs_online[i])
        recs_blended.append(recs_offline[i])

    # удаляем дубликаты
    recs_blended = dedup_ids(recs_blended)
    
    # оставляем только первые k рекомендаций
    recs_blended = recs_blended[:k]

    return {"recs": recs_blended}

@app.post("/recommendations_offline")
async def recommendations_offline(user_id: int, k: int = 100):
    """
    Возвращает список рекомендаций длиной k для пользователя user_id
    """

    recs = rec_store.get(user_id, k)

    return {"recs": recs}

@app.post("/recommendations_online")
async def recommendations_online(user_id: int, k: int = 100):
    """
    Возвращает список онлайн-рекомендаций длиной k для пользователя user_id
    """

    headers = {"Content-type": "application/json", "Accept": "text/plain"}

    # получаем список последних событий пользователя, возьмём три последних
    logger.info("Запрос последних событий пользователя")
    params = {"user_id": user_id, "k": 3}
    resp = requests.post(EVENTS_SERVICE_URL + "/get", headers=headers, params=params)
    events = resp.json()
    events = events["events"]
    logger.info(f"events {events}")
    # получаем список айтемов, похожих на последние три, с которыми взаимодействовал пользователь
    tracks  = []
    scores = []
    for track_id in events:
        # для каждого track_id получаем список похожих в similar

        resp = requests.post(RECS_ONLINE_SERVICE_URL +"/similar_track", headers=headers, params={"track_id": track_id, "k": 5})
        track_similar_items = resp.json()   

        tracks += track_similar_items["track_id_2"]
        scores += track_similar_items["score"]
    # сортируем похожие объекты по scores в убывающем порядке
 
    combined = list(zip(tracks, scores))
    combined = sorted(combined, key=lambda x: x[1], reverse=True)
    combined = [track for track, _ in combined]

    # удаляем дубликаты, чтобы не выдавать одинаковые рекомендации
    recs = dedup_ids(combined[:k])

    return {"recs": recs} 