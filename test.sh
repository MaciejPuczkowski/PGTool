PYTHONPATH="$(pwd)/src/:$(pwd)/tests/" \
pytest --asyncio-mode=strict -s "$@"