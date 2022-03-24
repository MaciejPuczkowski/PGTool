PYTHONPATH="$(pwd)/apgtool/:$(pwd)/tests/" \
pytest --asyncio-mode=strict -s "$@"