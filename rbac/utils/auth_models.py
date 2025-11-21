from pydantic import BaseModel, EmailStr
from typing import Optional

class Token(BaseModel):
    accessToken: str
    tokenType: str = "bearer"
    expiresIn: int
    username: str
    displayName: str
    role: str

class TokenRefreshRequest(BaseModel):
    accessToken: str

class TokenRefreshResponse(BaseModel):
    accessToken: str
    tokenType: str = "bearer"
    expiresIn: int
    message: str = "Token refreshed successfully"

class TokenData(BaseModel):
    username: Optional[str] = None

class UserLogin(BaseModel):
    username: str
    password: str

class UserResponse(BaseModel):
    username: str
    displayName: str
    roleStr: str
    role: str
    email: Optional[str] = None
    role_id: int
    client_id: Optional[int] = None
    role_name: str
    client_name: Optional[str] = None
    first_name: str
    last_name: str
    job_title: Optional[str] = None

class PasswordChange(BaseModel):
    currentPassword: str
    newPassword: str

class RefreshToken(BaseModel):
    refreshToken: str
    refreshToken: str 