"""
A FastAPI server that provides transparent request logging functionality.
This server logs all incoming HTTP requests, including their method, URL, headers, 
and body content, as well as the response status code. It serves as a debugging
and monitoring tool to truthfully reflect all request information received.

Logs are written both to 'requests.log' file and to stdout.
"""

from fastapi import FastAPI, Request
import logging
import uvicorn

app = FastAPI()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("requests.log"), logging.StreamHandler()],
)

@app.middleware("http")
async def log_requests(request: Request, call_next):
    logging.info("Request received:")
    logging.info(f"Method: {request.method}")
    logging.info(f"URL: {request.url}")
    headers = {key: value for key, value in request.headers.items()}
    logging.info(f"Headers: {headers}")
    body = await request.body()
    logging.info(f"Body: {body.decode('utf-8') if body else 'No body'}")
    
    # Process the request
    response = await call_next(request)
    logging.info(f"Response status: {response.status_code}")
    return response

@app.get("/")
async def read_root():
    return {"message": "Welcome to the logging server!"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
