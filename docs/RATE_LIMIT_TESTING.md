# Rate Limit Testing Guide

This guide explains how to test the rate limiting functionality in the API server.

## Overview

The API server implements rate limiting using an in-memory sliding window algorithm:

- **General API endpoints**: 60 requests per minute (default)
- **Upload endpoints**: 10 requests per minute (default)
- **Rate limiting key**: Username (if authenticated) or IP address (for anonymous)
- **Response**: HTTP 429 (Too Many Requests) when limit is exceeded
- **Headers**: Includes `X-RateLimit-Limit`, `X-RateLimit-Remaining`, and `X-RateLimit-Reset`

## Configuration

Rate limits can be configured via environment variables:

```bash
export API_RATE_LIMIT_PER_MIN=60        # General API limit
export UPLOAD_RATE_LIMIT_PER_MIN=10     # Upload endpoint limit
```

## Testing Methods

### Method 1: Using the Python Test Script (Recommended)

The `test_rate_limit.py` script provides comprehensive testing:

```bash
# Test general API endpoint (anonymous/IP-based)
python test_rate_limit.py --url http://localhost:8000 --limit 60

# Test with authentication token
python test_rate_limit.py --url http://localhost:8000 --token YOUR_JWT_TOKEN --limit 60

# Test upload endpoint (stricter limit)
python test_rate_limit.py --url http://localhost:8000 --upload --limit 10

# Test a specific endpoint
python test_rate_limit.py --url http://localhost:8000 --endpoint /api/clients --limit 60

# Fast testing with lower limit (for quick tests)
python test_rate_limit.py --url http://localhost:8000 --limit 5 --delay 0.05
```

### Method 2: Using cURL (Quick Test)

**Test with a loop (PowerShell):**

```powershell
# Test anonymous requests
$url = "http://localhost:8000/health"
for ($i=1; $i -le 65; $i++) {
    $response = Invoke-WebRequest -Uri $url -Method Get
    Write-Host "Request #$i : Status $($response.StatusCode) | Remaining: $($response.Headers['X-RateLimit-Remaining'])/$($response.Headers['X-RateLimit-Limit'])"
    Start-Sleep -Milliseconds 100
}
```

**Test with authentication (PowerShell):**

```powershell
$url = "http://localhost:8000/api/clients"
$token = "YOUR_JWT_TOKEN"
$headers = @{ "Authorization" = "Bearer $token" }

for ($i=1; $i -le 65; $i++) {
    try {
        $response = Invoke-WebRequest -Uri $url -Method Get -Headers $headers
        Write-Host "Request #$i : Status $($response.StatusCode) | Remaining: $($response.Headers['X-RateLimit-Remaining'])/$($response.Headers['X-RateLimit-Limit'])"
    } catch {
        if ($_.Exception.Response.StatusCode.value__ -eq 429) {
            $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
            $responseBody = $reader.ReadToEnd()
            Write-Host "Request #$i : RATE LIMITED (429)" -ForegroundColor Red
            Write-Host "Response: $responseBody" -ForegroundColor Red
            break
        }
    }
    Start-Sleep -Milliseconds 100
}
```

**Test with cURL (Bash/Unix):**

```bash
# Test anonymous requests
for i in {1..65}; do
    response=$(curl -s -w "\n%{http_code}" http://localhost:8000/health)
    status=$(echo "$response" | tail -n1)
    echo "Request #$i: Status $status"
    sleep 0.1
done
```

### Method 3: Using Apache Bench (ab)

```bash
# Install Apache Bench (if not available)
# Windows: Download from Apache website
# Linux: sudo apt-get install apache2-utils
# MacOS: Usually pre-installed

# Test with 100 requests, 10 concurrent
ab -n 100 -c 10 http://localhost:8000/health

# Test with authentication header
ab -n 100 -c 10 -H "Authorization: Bearer YOUR_TOKEN" http://localhost:8000/api/clients
```

### Method 4: Using Python requests (Quick Script)

```python
import requests
import time

url = "http://localhost:8000/health"
# url = "http://localhost:8000/api/clients"  # For authenticated endpoint
# headers = {"Authorization": "Bearer YOUR_TOKEN"}

for i in range(65):
    response = requests.get(url)  # Add headers=headers for auth
    print(f"Request #{i+1}: Status {response.status_code} | "
          f"Remaining: {response.headers.get('X-RateLimit-Remaining')}/{response.headers.get('X-RateLimit-Limit')}")
    
    if response.status_code == 429:
        print(f"Rate limit hit! Response: {response.json()}")
        break
    
    time.sleep(0.1)
```

## What to Look For

### Success Indicators

1. **Rate limit headers** are present in all responses:
   - `X-RateLimit-Limit`: Maximum requests allowed per minute
   - `X-RateLimit-Remaining`: Remaining requests in current window
   - `X-RateLimit-Reset`: Unix timestamp when the window resets

2. **Remaining count decreases** with each request:
   ```
   Request #1:  Remaining: 59/60
   Request #2:  Remaining: 58/60
   Request #3:  Remaining: 57/60
   ...
   Request #60: Remaining: 0/60
   Request #61: Status 429 (Rate Limited)
   ```

3. **HTTP 429 response** when limit is exceeded:
   ```json
   {
     "detail": "Rate limit exceeded. Maximum 60 requests per minute allowed.",
     "retry_after": 45
   }
   ```

4. **Retry-After header** indicates when to retry (in seconds)

### Testing Different Scenarios

1. **IP-based limiting (anonymous)**: Don't include Authorization header
2. **User-based limiting**: Include valid JWT token in Authorization header
3. **Upload endpoint**: Test with `/upload` endpoint (should limit at 10/min)
4. **Multiple users**: Test with different tokens to verify separate limits
5. **Window reset**: Wait 60 seconds after hitting limit and verify reset

## Troubleshooting

### Rate limit not being hit

- Check that the endpoint is not in the skip list (`/docs`, `/health`, etc.)
- Verify the server is running and middleware is active
- Check environment variables are set correctly
- Ensure you're making requests fast enough (within the 60-second window)

### Rate limit hit too early/late

- Check your environment variables: `API_RATE_LIMIT_PER_MIN` and `UPLOAD_RATE_LIMIT_PER_MIN`
- Verify the endpoint type (general vs upload)
- Check if you're authenticated (different limits per user)

### Headers not showing

- Rate limit headers are added to all responses, even 429 errors
- Check that you're reading response headers correctly in your tool
- Verify the middleware is active (should be after auth middleware)

## Example Test Session

```bash
# 1. Start the server
uvicorn server.APIServer:app --reload

# 2. In another terminal, run the test script
python test_rate_limit.py --url http://localhost:8000 --limit 60

# Expected output:
# ============================================================
# Testing Rate Limit
# ============================================================
# URL: http://localhost:8000/health
# Expected Limit: 60 requests/minute
# Authentication: No (IP-based)
# ============================================================
#
# Request #1: Status 200 | Remaining: 59/60
# Request #2: Status 200 | Remaining: 58/60
# ...
# Request #60: Status 200 | Remaining: 0/60
#
# ============================================================
# ✅ RATE LIMIT HIT on request #61
# ============================================================
# Status Code: 429
# Response: {'detail': 'Rate limit exceeded. Maximum 60 requests per minute allowed.', 'retry_after': 45}
# Retry-After: 45 seconds
# ============================================================
```

## Notes

- Rate limiting is **in-memory** and resets when the server restarts
- Each authenticated user has their own rate limit counter
- Anonymous users are rate-limited by IP address
- The sliding window ensures smooth rate limiting (not strict per-minute buckets)
- Rate limits apply to all endpoints except `/docs`, `/openapi.json`, `/redoc`, `/favicon.ico`, and `/health`

