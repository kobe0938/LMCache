pip install -r requirements.txt

uvicorn main:app --host 0.0.0.0 --port 8001 --reload

access@: http://127.0.0.1:8001/route

or use docker:
docker build -t vllm-router .
docker run -d -p 8000:8000 --name vllm-router-container vllm-router
docker run -it -p 8001:8001 43a4a59cf8b7676a69203148b531e9cf446fbf0032a8ca761b6ae405cc378f41
