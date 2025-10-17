import os
import boto3
from dotenv import load_dotenv

load_dotenv()

# Настройка S3 клиента для Yandex Cloud
s3 = boto3.client(
    's3',
    endpoint_url='https://storage.yandexcloud.net',
    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY']
)

# Настройки
BUCKET_NAME = os.environ.get('S3_BUCKET_NAME') 
S3_PATH = 'recsys/recommendations/' 
LOCAL_DIR = 'service/data' 
FILES_TO_DOWNLOAD = ['top_popular.parquet', 'similar.parquet', 'recommendations.parquet']

def download_files_from_s3():
    """
    Скачивает файлы из S3 бакета в локальную папку
    """
    # Создаем локальную папку, если она не существует
    os.makedirs(LOCAL_DIR, exist_ok=True)
    
    for file_name in FILES_TO_DOWNLOAD:
        s3_key = f"{S3_PATH}{file_name}"
        local_path = os.path.join(LOCAL_DIR, file_name)
        
        try:
            print(f"Скачивание {s3_key} в {local_path}...")
            s3.download_file(BUCKET_NAME, s3_key, local_path)
            print(f"Файл {file_name} успешно скачан")
            
        except Exception as e:
            print(f"Ошибка при скачивании {file_name}: {str(e)}")

if __name__ == "__main__":
    download_files_from_s3()