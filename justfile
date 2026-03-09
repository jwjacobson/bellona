# Default recipe - list available commands
default SESSION_NAME="bellona":
    @just --list

# Run app
run:
    uv run fastapi dev src/bellona/main.py

# Run tests
test *args:
    uv run pytest {{ args }}
