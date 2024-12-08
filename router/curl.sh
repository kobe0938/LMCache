curl "http://0.0.0.0:8080/v1/chat/completions" \
  -H "Content-Type: application/json" \
  -H "user_id: 123" \
  -d '{
      "model": "sao10k/l3-8b-lunaris",
      "stream": true,
      "messages": [
        {
          "role": "user",
          "content": "Hello!"
        }
      ]
    }'