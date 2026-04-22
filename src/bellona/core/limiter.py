from slowapi import Limiter
from slowapi.util import get_remote_address

from bellona.core.config import get_settings

settings = get_settings()

limiter = Limiter(
    key_func=get_remote_address,
    default_limits=[f"{settings.demo_global_rate_limit}/hour"]
)