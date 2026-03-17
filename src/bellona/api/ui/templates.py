from pathlib import Path

from fastapi.templating import Jinja2Templates

_BASE_DIR = Path(__file__).parent.parent.parent.parent.parent  # project root
templates = Jinja2Templates(directory=str(_BASE_DIR / "templates"))
