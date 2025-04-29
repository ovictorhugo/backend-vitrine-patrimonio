from typing import Optional

from pydantic import BaseModel, EmailStr, HttpUrl


class UserModel(BaseModel):
    displayName: str
    email: EmailStr
    uid: str = None
    photoURL: Optional[HttpUrl] = None
    provider: Optional[str] = None
    matricula: Optional[str] = None
    telephone: Optional[str] = None
    ramal: Optional[str] = None
