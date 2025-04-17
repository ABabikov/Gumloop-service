from fastapi import FastAPI, HTTPException
from gumloop import GumloopClient
from typing import List, Dict, Any
from pydantic import BaseModel
import asyncio
import json
import sys
import requests
import logging

print("Loading main.py")
# Инициализация FastAPI
app = FastAPI(
    title="Gumloop Integration Service",
    description="Сервис для взаимодействия с Gumloop API. Позволяет запускать потоки, проверять статусы и получать результаты.",
    version="1.0.0",
)

# Настройка логгера
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Инициализация клиента Gumloop
client = GumloopClient(
    api_key="c46a72e9459f42f58bbf87fe69a99e6a",
    user_id="ALc83X5S2uVc6E3vXOW3A9owJio1"
    # project_id="your_project_id"
)

@app.post("/run-flow/")
async def run_flow(flow_id: str, inputs: Dict[str, Any]):
    """
    Запускает поток (workflow) в Gumloop и возвращает результаты.
    
    :param flow_id: ID потока в Gumloop.
    :param inputs: Входные данные для потока.
    :return: Результат выполнения потока.
    """
    try:
        # Запуск потока в Gumloop
        output = client.run_flow(
            flow_id=flow_id,
            inputs=inputs
        )
        return {"status": "success", "output": output}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при выполнении потока: {str(e)}")

@app.post("/check-status/")
async def check_status(task_id: str):
    """
    Проверяет статус выполнения задачи в Gumloop.
    
    :param task_id: ID задачи в Gumloop.
    :return: Статус задачи.
    """
    try:
        # Проверка статуса задачи
        status = client.check_task_status(task_id=task_id)
        return {"status": "success", "task_status": status}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при проверке статуса: {str(e)}")

@app.post("/get-results/")
async def get_results(task_id: str):
    """
    Получает результаты выполнения задачи в Gumloop.
    
    :param task_id: ID задачи в Gumloop.
    :return: Результаты задачи.
    """
    try:
        # Получение результатов задачи
        results = client.get_task_results(task_id=task_id)
        return {"status": "success", "results": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при получении результатов: {str(e)}")


# @app.post("/run-and-wait/")
# async def run_and_wait(flow_id: str, inputs: Dict[str, Any], max_attempts: int = 30):
#     """
#     Запускает поток (workflow), проверяет статус выполнения каждые 2 секунды,
#     и возвращает результаты после завершения.

#     :param flow_id: ID потока в Gumloop.
#     :param inputs: Входные данные для потока.
#     :param max_attempts: Максимальное количество попыток проверки статуса (по умолчанию 30).
#     :return: Результат выполнения потока.
#     """
#     try:
#         # Запуск потока в Gumloop
#         task_response = client.run_flow(
#             flow_id=flow_id,
#             inputs=inputs
#         )
#         task_id = task_response.get("task_id")  # Предполагаем, что ответ содержит task_id

#         if not task_id:
#             raise HTTPException(status_code=500, detail="Не удалось получить task_id из ответа Gumloop.")

#         # Проверка статуса задачи с периодичностью 2 секунды
#         for attempt in range(max_attempts):
#             status_response = client.check_task_status(task_id=task_id)
#             task_status = status_response.get("status")

#             if task_status == "completed":
#                 # Получение результатов задачи
#                 results = client.get_task_results(task_id=task_id)
#                 return {"status": "success", "results": results}

#             elif task_status in ["failed", "error"]:
#                 raise HTTPException(status_code=500, detail=f"Задача завершилась с ошибкой: {task_status}")

#             # Ожидание 2 секунды перед следующей проверкой
#             await asyncio.sleep(2)

#         # Если превышено количество попыток
#         raise HTTPException(status_code=408, detail="Превышено время ожидания выполнения задачи.")

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Ошибка при выполнении потока: {str(e)}")


# Константы для Gumloop API
GUMLOOP_API_URL = "https://api.gumloop.com/api/v1"
AUTH_TOKEN = "c46a72e9459f42f58bbf87fe69a99e6a"
USER_ID = "ALc83X5S2uVc6E3vXOW3A9owJio1"

# Pydantic модель для pipeline_inputs
class PipelineInput(BaseModel):
    input_name: str
    value: str

# Pydantic модель для входных данных
class RunAndWaitRequest(BaseModel):
    saved_item_id: str
    pipeline_inputs: list[PipelineInput]

@app.post("/run-and-wait/")
async def run_and_wait(request: RunAndWaitRequest, max_attempts: int = 30):
    """
    Запускает поток (workflow), проверяет статус выполнения каждые 2 секунды,
    и возвращает результаты после завершения.

    :param request: Входные данные для запуска потока.
    :param max_attempts: Максимальное количество попыток проверки статуса (по умолчанию 30).
    :return: Результат выполнения потока (поле outputs).
    """
    try:
        # Шаг 1: Запуск потока
        start_pipeline_url = f"{GUMLOOP_API_URL}/start_pipeline"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {AUTH_TOKEN}"
        }
        payload = {
            "user_id": USER_ID,
            "saved_item_id": request.saved_item_id,
            "pipeline_inputs": [{"input_name": inp.input_name, "value": inp.value} for inp in request.pipeline_inputs]
        }

        logger.info(f"Запуск потока с payload: {payload}")
        response = requests.post(start_pipeline_url, json=payload, headers=headers, verify=False)
        logger.info(f"Ответ от Gumloop API (запуск потока): {response.status_code}, {response.text}")

        if response.status_code != 200:
            raise HTTPException(status_code=500, detail=f"Ошибка при запуске потока: {response.text}")

        # Извлечение run_id из ответа
        run_data = response.json()
        run_id = run_data.get("run_id")
        if not run_id:
            raise HTTPException(status_code=500, detail="Не удалось получить run_id из ответа Gumloop.")

        logger.info(f"Получен run_id: {run_id}")

        # Шаг 2: Проверка статуса выполнения
        get_run_status_url = f"{GUMLOOP_API_URL}/get_pl_run"
        for attempt in range(max_attempts):
            logger.info(f"Попытка {attempt + 1}/{max_attempts}: Проверка статуса для run_id: {run_id}")
            status_response = requests.get(
                get_run_status_url,
                params={"user_id": USER_ID, "run_id": run_id},
                headers=headers,
                verify=False
            )
            logger.info(f"Ответ от Gumloop API (проверка статуса): {status_response.status_code}, {status_response.text}")

            if status_response.status_code != 200:
                raise HTTPException(status_code=500, detail=f"Ошибка при проверке статуса: {status_response.text}")

            run_status = status_response.json()
            state = run_status.get("state")

            if state == "DONE":
                # Шаг 3: Возвращение результатов (поле outputs)
                outputs = run_status.get("outputs", {})
                logger.info(f"Задача завершена. Результаты: {outputs}")
                return {"status": "success", "outputs": outputs}

            elif state in ["FAILED", "ERROR"]:
                error_message = run_status.get("log", "Неизвестная ошибка")
                logger.error(f"Задача завершилась с ошибкой: {error_message}")
                raise HTTPException(status_code=500, detail=f"Задача завершилась с ошибкой: {error_message}")

            # Ожидание 2 секунды перед следующей проверкой
            await asyncio.sleep(2)

        # Если превышено количество попыток
        logger.error("Превышено время ожидания выполнения задачи.")
        raise HTTPException(status_code=408, detail="Превышено время ожидания выполнения задачи.")

    except Exception as e:
        logger.error(f"Произошла ошибка: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Ошибка при выполнении потока: {str(e)}")