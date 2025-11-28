import threading
import time
from collections import Counter
import json
import os
from dotenv import load_dotenv

import requests

load_dotenv()

URL = os.getenv("URL")
BEARER_TOKEN = os.getenv("BEARER_TOKEN")

#Body if needee
#BODY = os.getenv("BODY")

HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {BEARER_TOKEN}",
    "Cache-Control": "no-cache"
}

#requests count
CONCURRENCY = 10

#requests per thread
REQUESTS_PER_THREAD = 1

#Print response snippet if needed
PRINT_RESPONSE_SNIPPET = True

def make_request(start_barrier, results, thread_id):
    """
    Function that will be executed by each thread:
    - waits for a common start point through barrier
    - sends N requests in a row
    - saves results to shared list
    """
    # Wait for all threads to be ready
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

            # Try to read the response (can be json, can be text)
            try:
                data = response.json()
            except ValueError:
                data = response.text

            # Save result
            results.append({
                "thread": thread_id,
                "req_index": i,
                "status": status,
                "response": data,
            })

            if PRINT_RESPONSE_SNIPPET:
                # Print snippet if needed
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
    # Barrier waits for all threads to reach it, then releases them all at once
    start_barrier = threading.Barrier(CONCURRENCY + 1)  # +1 = main thread

    threads = []
    results = []  # shared list, where threads will write results

    # Start threads
    for t_id in range(CONCURRENCY):
        t = threading.Thread(target=make_request, args=(start_barrier, results, t_id))
        t.start()
        threads.append(t)

    print(f"Ready {CONCURRENCY} threads. Starting all at once through barrier...")
    time.sleep(1)

    # Release all threads at once
    start_barrier.wait()

    # Wait for all threads to finish
    for t in threads:
        t.join()

    print("\n=== RESULTS ===")

    #Summary by status codes
    statuses = Counter(r["status"] for r in results)
    print("Status codes:", dict(statuses))

    #If desired, you can view suspicious responses
    print("\nExamples of non-200 responses:")
    for r in results:
        if r["status"] != 200:
            print(f"- thread={r['thread']}, req={r['req_index']}, "
            f"status={r['status']}, body={str(r['response'])[:300]}")


if __name__ == "__main__":
    main()
