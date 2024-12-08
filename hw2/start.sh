mkdir -p dir1 dir2 dir3

python3 kv_store.py master localhost:8000 8000 dir1 localhost:8000 localhost:8001 localhost:8002 &
python3 kv_store.py replica localhost:8001 8001 dir2 localhost:8000 localhost:8001 localhost:8002 &
python3 kv_store.py replica localhost:8002 8002 dir3 localhost:8000 localhost:8001 localhost:8002 &