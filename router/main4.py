import time
import uuid
import threading
from collections import deque, defaultdict
from queue import Queue
from typing import Dict, Any
import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
import uvicorn
import asyncio

MACHINES = [
    "http://machine0:8000",
    "http://machine1:8000",
    "http://machine2:8000",
    "http://machine3:8000"
]
TIME_WINDOW = 10
TARGET_QPS = 0.2

app = FastAPI()

user_to_machine: Dict[str, int] = {}
request_table: Dict[str, Dict[str, Any]] = {}

requests_on_each_machine = [deque() for _ in range(len(MACHINES))]
machine_locks = [threading.Lock() for _ in MACHINES]
machine_processed_count = [0 for _ in MACHINES]
start_time = time.time()
user_request_count = defaultdict(int)

def calculate_current_qps(machine_id: int) -> float:
    current_time = time.time()
    count = 0
    with machine_locks[machine_id]:
        for req_id in reversed(requests_on_each_machine[machine_id]):
            arrival_time = request_table[req_id].get("arrival_time")
            if current_time - arrival_time <= TIME_WINDOW:
                count += 1
            else:
                break
    current_qps = count / TIME_WINDOW
    return current_qps

def calculate_total_qps(machine_id: int) -> float:
    elapsed = time.time() - start_time
    return machine_processed_count[machine_id] / elapsed if elapsed > 0 else 0.0

def machine_worker(machine_id: int):
    while True:
        time.sleep(0.1)
        current_qps = calculate_current_qps(machine_id)
        if current_qps > TARGET_QPS:
            # TODO: Still processing the request. But this's a good time to 1. send out the warning to add more GPUs 2. or investigate whether requests are to centerlized into certain GPUs
            pass


def print_status():
    while True:
        time.sleep(TIME_WINDOW)
        total_users_assigned_to_machine = [0]*len(MACHINES)
        inverse_map = defaultdict(list)
        for u, m in user_to_machine.items():
            inverse_map[m].append(u)
        for i in range(len(MACHINES)):
            total_users_assigned_to_machine[i] = len(inverse_map[i])
        for i in range(len(MACHINES)):
            qps = calculate_current_qps(i)
            total_qps = calculate_total_qps(i)
            print(f"Machine {i}: Current QPS={qps:.2f}, Total QPS={total_qps:.2f}, Assigned Users={total_users_assigned_to_machine[i]}")
        total_requests = sum(user_request_count.values())
        unique_users = len(user_request_count)
        print(f"Overall: Total Requests={total_requests}, Unique Users={unique_users}")
        print("====================")


async def process_request(request: Request, backend_url: str) -> asyncio.AsyncGenerator[bytes, None]:
    """
    Async generator to stream data from the backend server to the client.
    This replaces the synchronous process_request function and should be called
    directly by the route handler.
    """
    client = httpx.AsyncClient()
    async with client.stream(
        method=request.method,
        url=backend_url,
        headers=dict(request.headers),
        content=await request.body(),
    ) as backend_response:

        # Pass response headers to the client
        yield backend_response.headers, backend_response.status_code

        # Stream response content
        async for chunk in backend_response.aiter_bytes():
            yield chunk

    await client.aclose()


@app.post("/chat/completions")
async def route_request(request: Request):
    try:
        body = await request.json()
        user_id = request.headers.get("x-flow-user-id")
        if not user_id:
            raise HTTPException(status_code=400, detail="user_id header is missing")

        req_id = str(uuid.uuid4())
        arrival_time = time.time()
        request_table[req_id] = {
            "user_id": user_id,
            "body": body,
            "arrival_time": arrival_time,
            "allocated_machine_id": None,
            "execution_time": None
        }

        if user_id not in user_to_machine:
            machine_id = min(
                range(len(MACHINES)),
                key=lambda i: calculate_current_qps(i)
            )
            user_to_machine[user_id] = machine_id
        else:
            machine_id = user_to_machine[user_id]

        request_table[req_id]["allocated_machine_id"] = machine_id

        with machine_locks[machine_id]:
            requests_on_each_machine[machine_id].append(req_id)
            machine_processed_count[machine_id] += 1

        user_request_count[user_id] += 1

        backend_url = MACHINES[machine_id]
        stream_generator = process_request(request, backend_url)

        headers, status_code = await anext(stream_generator)

        return StreamingResponse(
                stream_generator,
                status_code=status_code,
                headers={key: value for key, value in headers.items() if key.lower() not in {"transfer-encoding"}},
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


for i in range(len(MACHINES)):
    t = threading.Thread(target=machine_worker, args=(i,), daemon=True)
    t.start()

status_thread = threading.Thread(target=print_status, daemon=True)
status_thread.start()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=9000)
