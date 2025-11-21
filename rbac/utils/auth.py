from datetime import datetime, timedelta, timezone
from typing import Optional, Union
from jose import JWTError, jwt
from passlib.context import CryptContext
from fastapi import HTTPException, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import os
import warnings
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Suppress bcrypt warnings and passlib warnings
warnings.filterwarnings("ignore", message=".*bcrypt.*", category=UserWarning)
warnings.filterwarnings("ignore", message=".*passlib.*", category=UserWarning)
import logging
logging.getLogger("passlib").setLevel(logging.ERROR)

# Password hashing context with bcrypt compatibility fix
def _create_password_context():
    """Create password context with fallback options"""
    try:
        # Try the full configuration first
        return CryptContext(schemes=["bcrypt"], deprecated="auto", bcrypt__rounds=12)
    except Exception as e:
        print(f"Warning: Full bcrypt initialization failed: {e}")
        try:
            # Try simpler bcrypt configuration
            return CryptContext(schemes=["bcrypt"])
        except Exception as e2:
            print(f"Warning: Simple bcrypt failed: {e2}")
            try:
                # Try with explicit version handling
                import bcrypt
                return CryptContext(schemes=["bcrypt"], bcrypt__default_ident="2b")
            except Exception as e3:
                print(f"Warning: Explicit bcrypt failed: {e3}")
                # Last resort - use argon2 or SHA256
                try:
                    return CryptContext(schemes=["argon2"])
                except:
                    return CryptContext(schemes=["sha256_crypt"])

pwd_context = _create_password_context()

# JWT Configuration
SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your-secret-key-change-in-production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "2880"))  # 48 hours = 2880 minutes

# Security scheme for token authentication
security = HTTPBearer()

def verifyPassword(plainPassword: str, hashedPassword: str) -> bool:
    """Verify a plain password against its hash"""
    try:
        return pwd_context.verify(plainPassword, hashedPassword)
    except Exception as e:
        # Handle bcrypt version compatibility issues
        print(f"Warning: Password verification error: {e}")
        
        # Try direct bcrypt verification if it's a bcrypt hash
        if hashedPassword.startswith('$2b$') or hashedPassword.startswith('$2a$') or hashedPassword.startswith('$2y$'):
            try:
                import bcrypt
                return bcrypt.checkpw(plainPassword.encode('utf-8'), hashedPassword.encode('utf-8'))
            except Exception as bcrypt_error:
                print(f"Direct bcrypt verification failed: {bcrypt_error}")
        
        # Handle SHA256 with salt format (hash:salt)
        if ':' in hashedPassword and len(hashedPassword.split(':')) == 2:
            try:
                import hashlib
                hash_part, salt = hashedPassword.split(':')
                computed_hash = hashlib.sha256((plainPassword + salt).encode()).hexdigest()
                import secrets
                return secrets.compare_digest(computed_hash, hash_part)
            except Exception as sha_error:
                print(f"SHA256 verification failed: {sha_error}")
        
        # Fallback to simple string comparison for legacy passwords
        import secrets
        try:
            return secrets.compare_digest(plainPassword, hashedPassword)
        except:
            return plainPassword == hashedPassword

def getPasswordHash(password: str) -> str:
    """Hash a password using bcrypt"""
    try:
        return pwd_context.hash(password)
    except Exception as e:
        # Handle bcrypt version compatibility issues
        print(f"Warning: Password hashing error: {e}")
        
        # Try direct bcrypt hashing
        try:
            import bcrypt
            # Generate salt and hash the password
            salt = bcrypt.gensalt()
            return bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')
        except Exception as bcrypt_error:
            print(f"Direct bcrypt hashing failed: {bcrypt_error}")
            
            # Try a simpler passlib context
            try:
                from passlib.context import CryptContext
                simple_context = CryptContext(schemes=["bcrypt"])
                return simple_context.hash(password)
            except Exception as simple_error:
                print(f"Simple passlib failed: {simple_error}")
                
                # Ultimate fallback to SHA256 with salt (not ideal but works)
                import hashlib
                import secrets
                salt = secrets.token_hex(16)
                return hashlib.sha256((password + salt).encode()).hexdigest() + ':' + salt

def createAccessToken(data: dict, expiresDelta: Optional[timedelta] = None):
    """Create JWT access token"""
    toEncode = data.copy()
    if expiresDelta:
        expire = datetime.now(timezone.utc) + expiresDelta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=15)
    # Convert datetime to timestamp for JWT
    toEncode.update({"exp": int(expire.timestamp())})
    encodedJwt = jwt.encode(toEncode, SECRET_KEY, algorithm=ALGORITHM)
    return encodedJwt

def extendTokenExpiry(token: str, additionalMinutes: int = 30) -> Optional[str]:
    """Extend the expiry time of an existing JWT token"""
    try:
        # Decode the existing token without verification to get the payload
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM], options={"verify_exp": False})
        
        # Get current expiry time
        currentExp = payload.get("exp")
        if currentExp:
            # Convert to datetime and add additional time
            currentExpDatetime = datetime.fromtimestamp(currentExp, tz=timezone.utc)
            newExp = currentExpDatetime + timedelta(minutes=additionalMinutes)
            # Convert back to timestamp for JWT
            payload["exp"] = int(newExp.timestamp())
            
            # Create new token with extended expiry
            newToken = jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)
            return newToken
        return None
    except JWTError:
        return None

def getTokenExpiryTime(token: str) -> Optional[datetime]:
    """Get the expiry time of a JWT token"""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM], options={"verify_exp": False})
        exp = payload.get("exp")
        if exp:
            return datetime.fromtimestamp(exp, tz=timezone.utc)
        return None
    except JWTError:
        return None

def isTokenExpiringSoon(token: str, minutesThreshold: int = 5) -> bool:
    """Check if a token is expiring within the specified minutes"""
    try:
        expiryTime = getTokenExpiryTime(token)
        if expiryTime:
            timeUntilExpiry = expiryTime - datetime.now(timezone.utc)
            return timeUntilExpiry.total_seconds() <= (minutesThreshold * 60)
        return True  # If we can't determine expiry, assume it's expiring soon
    except Exception:
        return True

def verifyToken(token: str) -> Optional[dict]:
    """Verify and decode a JWT token"""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            return None
        return payload
    except JWTError:
        return None

def getCurrentUser(credentials: HTTPAuthorizationCredentials = Depends(security)) -> str:
    """Get current user from JWT token"""
    token = credentials.credentials
    payload = verifyToken(token)
    if payload is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    username: str = payload.get("sub")
    if username is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return username

def authenticateUser(username: str, password: str) -> Optional[dict]:
    """Authenticate user with username or email and password using database"""
    try:
        # Import DatabaseManager for database authentication
        import sys
        import os
        # Add the root directory to Python path for Docker compatibility
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        from database_models import DatabaseManager
        
        # Try database authentication first
        db_manager = DatabaseManager()
        # Support login via email or username with graceful fallbacks
        user = None
        if '@' in username:
            # 1) Try email lookup
            user = db_manager.get_user_by_email(username)
            # 2) Try exact string as username (some systems store email in username)
            if not user:
                user = db_manager.get_user_by_username(username)
            # 3) Try local-part before '@' as username
            if not user:
                local_part = username.split('@', 1)[0]
                user = db_manager.get_user_by_username(local_part)
        else:
            user = db_manager.get_user_by_username(username)
        
        if user and user.is_active:
            # Check if password is hashed (new format) or plain text (old format)
            storedPassword = user.password_hash
            
            # If password starts with $2b$ (bcrypt hash), verify against hash
            if storedPassword.startswith('$2b$'):
                if not verifyPassword(password, storedPassword):
                    return None
            else:
                # Legacy plain text password comparison
                import secrets
                if not secrets.compare_digest(password, storedPassword):
                    return None
            
            # Return user data in expected format
            return {
                'username': user.username,
                'displayName': user.display_name,
                'roleStr': user.role.role_name if user.role else 'Unknown',
                'role': user.role.role_code if user.role else 'unknown',
                'password': user.password_hash,
                'is_active': user.is_active
            }
        
        # No fallback - return None if user not found in database
        return None
        
    except Exception as e:
        print(f"Error in authenticateUser: {e}")
        # No fallback - return None on database errors
        return None 