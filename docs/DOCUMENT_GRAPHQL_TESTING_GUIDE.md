# Document GraphQL API Testing Guide

## 🚀 Quick Start

The Document GraphQL API is available at: **`/v2`** endpoint

### Prerequisites
1. Server must be running
2. You need a valid JWT token (same as REST API authentication)
3. GraphQL playground available at: `http://localhost:8000/v2` (when server is running)

---

## 🔐 Authentication

The GraphQL API uses the same JWT authentication as the REST API.

### Step 1: Get JWT Token

**Using REST API:**
```bash
curl -X POST http://localhost:8000/login \
  -H "Content-Type: application/json" \
  -d '{"username": "your_username", "password": "your_password"}'
```

**Response:**
```json
{
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "user": {...}
}
```

### Step 2: Use Token in GraphQL Requests

Add the token to the HTTP headers:
```
Authorization: Bearer YOUR_JWT_TOKEN
```

---

## 🧪 Testing Methods

### Method 1: GraphQL Playground (Browser)

1. **Start your server**
   ```bash
   python start_server.py
   ```

2. **Open GraphQL Playground**
   - Navigate to: `http://localhost:8000/v2`
   - The interactive GraphQL playground will open

3. **Set Authentication Header**
   - Click "HTTP HEADERS" at the bottom
   - Add:
   ```json
   {
     "Authorization": "Bearer YOUR_JWT_TOKEN"
   }
   ```

4. **Run your queries!**

---

### Method 2: cURL (Command Line)

**Example:**
```bash
curl -X POST http://localhost:8000/v2 \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -d '{
    "query": "{ documents { success message total documents { id name type status } } }"
  }'
```

---

### Method 3: Python Script

Save this as `test_document_graphql.py`:

```python
import requests
import json

# Configuration
BASE_URL = "http://localhost:8000"
USERNAME = "your_username"
PASSWORD = "your_password"

# 1. Get JWT Token
def get_token():
    response = requests.post(
        f"{BASE_URL}/login",
        json={"username": USERNAME, "password": PASSWORD}
    )
    return response.json()["token"]

# 2. Run GraphQL Query
def run_query(query, token, variables=None):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    
    response = requests.post(
        f"{BASE_URL}/v2",
        json=payload,
        headers=headers
    )
    return response.json()

# Example usage
if __name__ == "__main__":
    # Get token
    token = get_token()
    print("✅ Authenticated successfully")
    
    # Query documents
    query = """
    query {
      documents(limit: 5) {
        success
        message
        total
        documents {
          id
          name
          type
          status
          createdBy
          uploadDate
        }
      }
    }
    """
    
    result = run_query(query, token)
    print("\n📄 Documents:", json.dumps(result, indent=2))
```

---

## 📝 Example Queries & Mutations

### Query 1: Get All Documents

```graphql
query GetAllDocuments {
  documents(limit: 10, offset: 0) {
    success
    message
    total
    documents {
      id
      name
      type
      path
      size
      status
      fundId
      uploadDate
      replay
      createdBy
      metadata
      isActive
      createdAt
      updatedAt
    }
  }
}
```

### Query 2: Get Documents with Filters

```graphql
query GetFilteredDocuments {
  documents(
    filter: {
      type: "CapitalCall"
      status: "completed"
      replay: false
      isActive: true
    }
    limit: 20
    offset: 0
  ) {
    success
    message
    total
    documents {
      id
      name
      type
      status
      createdBy
      uploadDate
    }
  }
}
```

### Query 3: Get Single Document by ID

```graphql
query GetDocument {
  document(documentId: 1) {
    success
    message
    document {
      id
      name
      type
      path
      size
      status
      fundId
      uploadDate
      replay
      createdBy
      metadata
      isActive
      createdAt
      updatedAt
    }
  }
}
```

### Query 4: Get Documents by Fund

```graphql
query GetDocumentsByFund {
  documentsByFund(fundId: 1, limit: 10, offset: 0) {
    success
    message
    total
    documents {
      id
      name
      type
      status
      uploadDate
      createdBy
    }
  }
}
```

### Query 5: Search Documents by Name

```graphql
query SearchDocuments {
  documents(
    filter: {
      name: "capital"  # Partial match, case-insensitive
    }
    limit: 10
  ) {
    success
    message
    total
    documents {
      id
      name
      type
      status
    }
  }
}
```

---

## ✏️ Mutation Examples

### Mutation 1: Create a Document

```graphql
mutation CreateDocument {
  createDocument(input: {
    name: "capital_call_2024.pdf"
    type: "CapitalCall"
    path: "/documents/capital_call_2024.pdf"
    size: 524288
    status: "pending"
    fundId: 1
    replay: false
    metadata: {
      "extractedBy": "frame_engine",
      "confidence": 0.95
    }
  }) {
    success
    message
    document {
      id
      name
      type
      status
      createdBy
      uploadDate
    }
  }
}
```

**Note:** The `createdBy` field will be automatically set to the authenticated user's username.

### Mutation 2: Update a Document

```graphql
mutation UpdateDocument {
  updateDocument(
    documentId: 1
    input: {
      status: "completed"
      metadata: {
        "processed": true,
        "extractionComplete": true
      }
    }
  ) {
    success
    message
    document {
      id
      name
      status
      metadata
      updatedAt
    }
  }
}
```

### Mutation 3: Update Document with Replay Flag

```graphql
mutation MarkForReplay {
  updateDocument(
    documentId: 1
    input: {
      replay: true
      status: "pending"
    }
  ) {
    success
    message
    document {
      id
      name
      status
      replay
    }
  }
}
```

### Mutation 4: Soft Delete a Document

```graphql
mutation DeleteDocument {
  deleteDocument(documentId: 1) {
    success
    message
  }
}
```

---

## 🔍 Complex Query Examples

### Example 1: Get Documents with Full Details

```graphql
query GetDocumentsWithFullDetails {
  documents(
    filter: {
      status: "completed"
      isActive: true
    }
    limit: 5
  ) {
    success
    message
    total
    documents {
      id
      name
      type
      path
      size
      status
      fundId
      uploadDate
      replay
      createdBy
      metadata
      isActive
      createdAt
      updatedAt
    }
  }
}
```

### Example 2: Pagination Example

```graphql
# First page
query GetDocumentsPage1 {
  documents(limit: 10, offset: 0) {
    success
    total
    documents { id name }
  }
}

# Second page
query GetDocumentsPage2 {
  documents(limit: 10, offset: 10) {
    success
    total
    documents { id name }
  }
}
```

### Example 3: Get Documents by User

```graphql
query GetMyDocuments {
  documents(
    filter: {
      createdBy: "zeeshan"
      isActive: true
    }
  ) {
    success
    message
    total
    documents {
      id
      name
      type
      uploadDate
      status
    }
  }
}
```

---

## 🎯 Testing Scenarios

### Scenario 1: Complete Document Lifecycle

**Step 1: Create Document**
```graphql
mutation {
  createDocument(input: {
    name: "test_document.pdf"
    type: "Statement"
    path: "/test/test_document.pdf"
    status: "pending"
  }) {
    success
    document { id }
  }
}
```

**Step 2: Update to Processing**
```graphql
mutation {
  updateDocument(documentId: 1, input: { status: "processing" }) {
    success
    document { status }
  }
}
```

**Step 3: Update to Completed**
```graphql
mutation {
  updateDocument(documentId: 1, input: { 
    status: "completed"
    metadata: { "extractedFields": 25 }
  }) {
    success
    document { status metadata }
  }
}
```

**Step 4: Mark for Replay**
```graphql
mutation {
  updateDocument(documentId: 1, input: { replay: true }) {
    success
    document { replay }
  }
}
```

**Step 5: Delete Document**
```graphql
mutation {
  deleteDocument(documentId: 1) {
    success
    message
  }
}
```

### Scenario 2: Filter and Search

```graphql
query FilterDocuments {
  # Get all pending CapitalCall documents
  pendingCapCalls: documents(filter: {
    type: "CapitalCall"
    status: "pending"
  }) {
    total
    documents { id name }
  }
  
  # Get all documents for fund 1
  fund1Docs: documentsByFund(fundId: 1) {
    total
    documents { id name }
  }
  
  # Search by name
  searchResults: documents(filter: {
    name: "2024"
  }) {
    total
    documents { id name uploadDate }
  }
}
```

---

## 🐛 Troubleshooting

### Issue 1: "Authentication required" Error

**Problem:** GraphQL returns authentication error

**Solution:**
1. Verify your JWT token is valid
2. Check the Authorization header format: `Bearer YOUR_TOKEN`
3. Ensure token hasn't expired (get a new one)

### Issue 2: GraphQL Playground Not Loading

**Problem:** `/v2` endpoint not accessible

**Solution:**
1. Check server is running: `python start_server.py`
2. Verify strawberry-graphql is installed: `pip install strawberry-graphql`
3. Check server logs for errors

### Issue 3: Query Returns Empty Results

**Problem:** Documents query returns empty array

**Solution:**
1. Check if documents exist in database
2. Verify `isActive` filter (defaults to `true`)
3. Try removing filters to see all documents

---

## 📊 Response Format

All GraphQL responses follow this pattern:

```json
{
  "data": {
    "documents": {
      "success": true,
      "message": "Retrieved 5 documents",
      "total": 25,
      "documents": [
        {
          "id": 1,
          "name": "document.pdf",
          "type": "CapitalCall",
          "status": "completed",
          ...
        }
      ]
    }
  }
}
```

**Error Response:**
```json
{
  "data": {
    "documents": {
      "success": false,
      "message": "Error message here",
      "documents": [],
      "total": 0
    }
  }
}
```

---

## 🔒 Security Notes

1. ✅ All queries/mutations require authentication
2. ✅ JWT tokens are validated on every request
3. ✅ Soft delete preserves data (sets `isActive = false`)
4. ✅ `createdBy` automatically set from authenticated user
5. ✅ Same security model as REST API

---

## 📚 Additional Resources

- **GraphQL Playground:** `http://localhost:8000/v2`
- **REST API Docs:** Check `GRAPHQL_API_DOCUMENTATION.md`
- **Schema Definition:** `schema/graphql_document_schema.py`
- **TypeScript Types:** `graphql_types.ts`

---

## 💡 Tips

1. **Use GraphQL Playground** for interactive testing and auto-completion
2. **Test authentication first** before testing document queries
3. **Use variables** for dynamic values in queries
4. **Check total count** to verify pagination
5. **Monitor server logs** for debugging

---

## 🎓 Example: Full Test Session

```python
# test_session.py
import requests
import json

BASE_URL = "http://localhost:8000"

# 1. Login
login_response = requests.post(f"{BASE_URL}/login", json={
    "username": "admin",
    "password": "password"
})
token = login_response.json()["token"]
headers = {"Authorization": f"Bearer {token}"}

# 2. Create Document
create_mutation = """
mutation {
  createDocument(input: {
    name: "test.pdf"
    type: "CapitalCall"
    path: "/test.pdf"
    status: "pending"
  }) {
    success
    document { id name }
  }
}
"""

response = requests.post(
    f"{BASE_URL}/v2",
    json={"query": create_mutation},
    headers=headers
)
doc_id = response.json()["data"]["createDocument"]["document"]["id"]
print(f"Created document ID: {doc_id}")

# 3. Query Documents
query = """
query {
  documents(limit: 5) {
    success
    total
    documents { id name status }
  }
}
"""

response = requests.post(
    f"{BASE_URL}/v2",
    json={"query": query},
    headers=headers
)
print("Documents:", json.dumps(response.json(), indent=2))

# 4. Update Document
update_mutation = f"""
mutation {{
  updateDocument(documentId: {doc_id}, input: {{
    status: "completed"
  }}) {{
    success
    document {{ id status }}
  }}
}}
"""

response = requests.post(
    f"{BASE_URL}/v2",
    json={"query": update_mutation},
    headers=headers
)
print("Updated:", json.dumps(response.json(), indent=2))
```

---

**Happy Testing! 🚀**

