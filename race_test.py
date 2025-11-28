import threading
import time
from collections import Counter
import json
import os
from dotenv import load_dotenv

import requests

load_dotenv()

# ================== НАСТРОЙКИ ==================

URL = os.getenv("URL")
BEARER_TOKEN = os.getenv("BEARER_TOKEN")
# Тело запроса - пример перевода денег / бронирования / изменения ресурс

HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {BEARER_TOKEN}",
    "Cache-Control": "no-cache"
}

# Кол-во одновременных запросов
CONCURRENCY = 10

# Сколько раз каждый поток отправит запрос
REQUESTS_PER_THREAD = 1

# Печатать ли тело ответа
PRINT_RESPONSE_SNIPPET = True

def make_request(start_barrier, results, thread_id):
    """
    Функция, которую будет выполнять каждый поток:
    - ждёт общей точки старта через barrier
    - шлёт N запросов подряд
    - складывает результаты в shared-список
    """
    # Ждем, пока все потоки будут готовы
    start_barrier.wait()

    for i in range(REQUESTS_PER_THREAD):
        try:
            response = requests.post(
                URL,
                headers=HEADERS,
                data=None,
                timeout=10
            )

            status = response.status_code

            # Попробуем прочитать ответ (может быть json, может нет)
            try:
                data = response.json()
            except ValueError:
                data = response.text

            # Сохраняем результат
            results.append({
                "thread": thread_id,
                "req_index": i,
                "status": status,
                "response": data,
            })

            if PRINT_RESPONSE_SNIPPET:
                # печатаем кратко
                print(f"[{thread_id}:{i}] status={status}, body={str(data)[:200]}")

        except Exception as e:
            results.append({
                "thread": thread_id,
                "req_index": i,
                "status": "EXCEPTION",
                "response": str(e),
            })
            print(f"[{thread_id}:{i}] ERROR: {e}")


def main():
    # Barrier ждёт, пока все потоки дойдут до него, затем отпускает разом
    start_barrier = threading.Barrier(CONCURRENCY + 1)  # +1 = главный поток

    threads = []
    results = []  # shared-список, в который потоки будут писать результаты

    # Стартуем потоки
    for t_id in range(CONCURRENCY):
        t = threading.Thread(target=make_request, args=(start_barrier, results, t_id))
        t.start()
        threads.append(t)

    print(f"Готово {CONCURRENCY} потоков. Стартуем все одновременно через barrier...")
    time.sleep(1)

    # Отпускаем все потоки одновременно
    start_barrier.wait()

    # Ждём окончания всех потоков
    for t in threads:
        t.join()

    print("\n=== РЕЗУЛЬТАТЫ ===")

    # Сводка по статус-кодам
    statuses = Counter(r["status"] for r in results)
    print("Статусы ответов:", dict(statuses))

    # При желании можно посмотреть подозрительные ответы
    print("\nПримеры не-200 ответов:")
    for r in results:
        if r["status"] != 200:
            print(f"- thread={r['thread']}, req={r['req_index']}, "
            f"status={r['status']}, body={str(r['response'])[:300]}")


if __name__ == "__main__":
    main()
