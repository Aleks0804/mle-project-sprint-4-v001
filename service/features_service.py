import logging
from contextlib import asynccontextmanager

import pandas as pd
from fastapi import FastAPI

from settings import (
    ONLINE_RECS_PATH
    )

logger = logging.getLogger("uvicorn.error")

class SimilarTracks:

    def __init__(self):

        self._similar_trakcs = None

    def load(self, path, **kwargs):
        """
        Загружаем данные из файла
        """

        logger.info(f"Загрузка данных {path}")
        self._similar_trakcs = pd.read_parquet(path, **kwargs)
        self._similar_trakcs = self._similar_trakcs.set_index("track_id_1")
        logger.info(f"Данные загружены {path}")

    def get(self, track_id: int, k: int = 10):
        """
        Возвращает список похожих объектов
        """

        try:
            logger.info(f"Поиск похожих треков: {track_id}")
            i2i = self._similar_trakcs.loc[track_id].head(k)
            i2i = i2i[["track_id_2", "score"]].to_dict(orient="list")
            logger.info(f"Похожие треки найдены")
        except KeyError:
            logger.error("Рекомендаций не найдено")
            i2i = {"track_id_2": [], "score": {}}

        return i2i

sim_tracks_store = SimilarTracks()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # код ниже (до yield) выполнится только один раз при запуске сервиса
    sim_tracks_store.load(
        ONLINE_RECS_PATH,
        columns=["track_id_1", "track_id_2", "score"],
    )
    logger.info("Сервис features_service запущен!")
    # код ниже выполнится только один раз при остановке сервиса
    yield

# создаём приложение FastAPI
app = FastAPI(title="features", lifespan=lifespan)

@app.post("/similar_track")
async def recommendations(track_id: int, k: int = 10):
    """
    Возвращает список похожих объектов длиной k для item_id
    """

    i2i = sim_tracks_store.get(track_id, k)

    return i2i