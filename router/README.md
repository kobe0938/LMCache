pip install -r requirements.txt

uvicorn main:app --host 0.0.0.0 --port 8001 --reload

access@: http://127.0.0.1:8000/route