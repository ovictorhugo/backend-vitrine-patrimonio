import smtplib
from typing import Annotated

from fastapi import Depends
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.ext.asyncio import AsyncSession

from vitrine.core.database import get_session
from vitrine.core.mail import get_smtp
from vitrine.core.security import get_current_user
from vitrine.models import User

Session = Annotated[AsyncSession, Depends(get_session)]
CurrentUser = Annotated[User, Depends(get_current_user)]
OAuth2Form = Annotated[OAuth2PasswordRequestForm, Depends()]
Mail = Annotated[smtplib.SMTP, Depends(get_smtp)]
