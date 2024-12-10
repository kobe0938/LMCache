import time
import uuid
import threading
from collections import deque, defaultdict
from queue import Queue
from typing import Dict, Any
import httpx
from fastapi import FastAPI, HTTPException, Request, Response
import uvicorn

MACHINES = [
    "http://machine0:8000",
    "http://machine1:8000",
    "http://machine2:8000",
    "http://machine3:8000"
]
TIME_WINDOW = 10
TARGET_QPS = 1.8

app = FastAPI()

user_to_machine: Dict[str, int] = {}
request_table: Dict[str, Dict[str, Any]] = {}

requests_on_each_machine = [deque() for _ in range(len(MACHINES))]
queues_on_each_machine = [Queue() for _ in MACHINES]
machine_locks = [threading.Lock() for _ in MACHINES]
machine_processed_count = [0 for _ in MACHINES]
start_time = time.time()
user_request_count = defaultdict(int)

def calculate_current_qps(machine_id: int) -> float:
    current_time = time.time()
    count = 0
    with machine_locks[machine_id]:
        for req_id in reversed(requests_on_each_machine[machine_id]):
            exec_time = request_table[req_id].get("execution_time")
            if exec_time is None:
                continue
            if current_time - exec_time <= TIME_WINDOW:
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
        if current_qps < TARGET_QPS:
            if not queues_on_each_machine[machine_id].empty():
                req_id = queues_on_each_machine[machine_id].get()
                process_request(machine_id, req_id)

def process_request(machine_id: int, req_id: str):
    user_id = request_table[req_id]["user_id"]
    body = request_table[req_id]["body"]
    machine_url = MACHINES[machine_id]
    headers = {"user_id": user_id}
    try:
        with httpx.Client() as client:
            response = client.post(machine_url, json=body, headers=headers)
            request_table[req_id]["execution_time"] = time.time()
            with machine_locks[machine_id]:
                machine_processed_count[machine_id] += 1
    except Exception as e:
        print(f"Error processing request {req_id} on machine {machine_id}: {e}")

def print_status():
    while True:
        time.sleep(10)
        total_users_assigned_to_machine = [0]*len(MACHINES)
        inverse_map = defaultdict(list)
        for u, m in user_to_machine.items():
            inverse_map[m].append(u)
        for i in range(len(MACHINES)):
            total_users_assigned_to_machine[i] = len(inverse_map[i])
        for i in range(len(MACHINES)):
            qps = calculate_current_qps(i)
            total_q = queues_on_each_machine[i].qsize()
            total_qps = calculate_total_qps(i)
            print(f"Machine {i}: Current QPS={qps:.2f}, Total QPS={total_qps:.2f}, Queue Size={total_q}, Assigned Users={total_users_assigned_to_machine[i]}")
        total_requests = sum(user_request_count.values())
        unique_users = len(user_request_count)
        print(f"Overall: Total Requests={total_requests}, Unique Users={unique_users}")
        print("====================")

@app.post("/route")
async def route_request(request: Request):
    try:
        body = await request.json()
        user_id = request.headers.get("user_id")
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
            machine_id = min(range(len(MACHINES)), key=lambda i: queues_on_each_machine[i].qsize())
            user_to_machine[user_id] = machine_id
        else:
            machine_id = user_to_machine[user_id]
        request_table[req_id]["allocated_machine_id"] = machine_id
        with machine_locks[machine_id]:
            requests_on_each_machine[machine_id].append(req_id)
        queues_on_each_machine[machine_id].put(req_id)
        user_request_count[user_id] += 1
        return {"status": "queued", "request_id": req_id, "assigned_machine": machine_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

for i in range(len(MACHINES)):
    t = threading.Thread(target=machine_worker, args=(i,), daemon=True)
    t.start()

status_thread = threading.Thread(target=print_status, daemon=True)
status_thread.start()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
