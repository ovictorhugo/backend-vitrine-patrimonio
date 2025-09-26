from typing import Any

from vitrine.models import (
    User,
)


async def send_email(user: User, content: Any): ...
