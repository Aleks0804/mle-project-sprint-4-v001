from service import settings
import requests
import logging
import sys

logger = logging.getLogger('rec_s_test')
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(name)s | %(levelname)-8s | %(message)s')

file_log = logging.FileHandler('test_service.log')
file_log.setLevel(logging.INFO)
file_log.setFormatter(formatter)

console_log = logging.StreamHandler(sys.stdout)
console_log.setLevel(logging.INFO)
console_log.setFormatter(formatter)

logger.addHandler(file_log)
logger.addHandler(console_log)

headers = {"Content-type": "application/json", "Accept": "text/plain"}

def send_request(service, endpoint, params, headers=headers):
    response = requests.post(service+endpoint, headers=headers, params=params)

    logger.info(f"Request: url='{response.request.url}', method='{response.request.method}'")
    logger.info(f"Response: status_code='{response.status_code}', data='{response.text}'")

    if response.status_code == 200:
        response=response.json()
    else:
        response = []
        print(f"Ошибка! {response.status_code}")

    return response

def user_recs_check(user_id):
    logger.info(f"Проверка для пользователя, user_id = {user_id}")
    params_user_recs_check = {"user_id": user_id, "k": 5}
    response_user_recs_check = send_request(
        service=settings.RECS_SERVICE_URL,
        endpoint="/recommendations",
        params=params_user_recs_check
    )
    return response_user_recs_check

# для пользователя без персональных рекомендаций, (user_id = 5, user_id = 6)
logger.info('Проверка для пользователей без персональных рекомендаций (результаты должны быть одинаковы)')
print(user_recs_check(user_id = 5))
print(user_recs_check(user_id = 6))
logger.info('-'*50)

# для пользователя с персональными рекомендациями, но без онлайн-истории, (user_id = 4)
logger.info('Проверка для пользователя с персональными рекомендациями, но без онлайн-истории (результат должен оличаться от предыдущих) ')
print(user_recs_check(user_id = 4))
logger.info('-'*50)

# для пользователя с персональными рекомендациями и онлайн-историей.
logger.info('Генерируем историю для пользователя без истории и с перс. рекомендациями')

# Создание истории пользователю user_id = 4, в истоию передадим результат пользователей без перс. рекомендаций (top_popular)
hist_list = [53404,33311009,178529,35505245,24692821]

for track in hist_list:
    response = send_request(
        service=settings.EVENTS_SERVICE_URL,
        endpoint="/put",
        params={"user_id": 4, "track_id": track}
    )

logger.info('-'*50)

logger.info('Проверка для пользователя с персональными рекомендациями, и с историей ')
print(user_recs_check(user_id = 4))
logger.info('-'*50)
