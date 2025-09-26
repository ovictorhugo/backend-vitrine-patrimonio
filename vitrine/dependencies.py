import smtplib
from typing import Annotated

from fastapi import Depends
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.ext.asyncio import AsyncSession

from vitrine.database import get_session
from vitrine.mail import get_smtp
from vitrine.models import User
from vitrine.security import get_current_user

Session = Annotated[AsyncSession, Depends(get_session)]
CurrentUser = Annotated[User, Depends(get_current_user)]
OAuth2Form = Annotated[OAuth2PasswordRequestForm, Depends()]
Mail = Annotated[smtplib.SMTP, Depends(get_smtp)]
