# 🚀 Quick Start: Testing Document GraphQL API

## ⚡ 3-Step Setup

### Step 1: Start the Server
```bash
python start_server.py
```

### Step 2: Update Test Credentials
Edit `test_document_graphql.py`:
```python
USERNAME = "your_username"  # Replace with your actual username
PASSWORD = "your_password"  # Replace with your actual password
```

### Step 3: Run Tests
```bash
python test_document_graphql.py
```

---

## 🌐 Option 1: GraphQL Playground (Recommended)

**Best for:** Interactive testing and exploration

1. **Open Browser:** `http://localhost:8000/v2`
2. **Login first** (using REST API or cURL):
   ```bash
   curl -X POST http://localhost:8000/login \
     -H "Content-Type: application/json" \
     -d '{"username":"admin","password":"password"}'
   ```
3. **Copy the token** from the response
4. **In GraphQL Playground**, click "HTTP HEADERS" and add:
   ```json
   {
     "Authorization": "Bearer YOUR_TOKEN_HERE"
   }
   ```
5. **Run a test query:**
   ```graphql
   query {
     documents(limit: 5) {
       success
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

## 🐍 Option 2: Python Script (Automated)

**Best for:** Automated testing and CI/CD

Run the complete test suite:
```bash
python test_document_graphql.py
```

This will automatically:
- ✅ Authenticate
- ✅ Get all documents
- ✅ Create a test document
- ✅ Query single document
- ✅ Update the document
- ✅ Filter documents
- ✅ Delete the document

---

## 📮 Option 3: Postman Collection

**Best for:** API testing and sharing with team

1. **Import Collection:**
   - Open Postman
   - Import → `Document_GraphQL_API.postman_collection.json`

2. **Set Variables:**
   - Click on collection → Variables tab
   - Set `base_url`: `http://localhost:8000`

3. **Login:**
   - Run "Auth → Login" request
   - Token will be auto-saved to environment

4. **Test Queries:**
   - All requests now automatically use the token
   - Run any query/mutation from the collection

---

## 🔥 Quick Examples

### Get All Documents
```graphql
{
  documents {
    success
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

### Create Document
```graphql
mutation {
  createDocument(input: {
    name: "test.pdf"
    type: "CapitalCall"
    path: "/test.pdf"
    status: "pending"
  }) {
    success
    document {
      id
      name
    }
  }
}
```

### Update Document Status
```graphql
mutation {
  updateDocument(
    documentId: 1
    input: { status: "completed" }
  ) {
    success
    document {
      id
      status
    }
  }
}
```

---

## 🐛 Common Issues

### Issue: "Authentication required"
**Fix:** Get a new token and add to headers:
```
Authorization: Bearer YOUR_TOKEN
```

### Issue: Empty results
**Fix:** Check if documents exist and `isActive = true`

### Issue: GraphQL endpoint not found
**Fix:** Verify server is running and strawberry-graphql is installed

---

## 📚 Documentation Files

- **Full Testing Guide:** `DOCUMENT_GRAPHQL_TESTING_GUIDE.md`
- **Python Test Script:** `test_document_graphql.py`
- **Postman Collection:** `Document_GraphQL_API.postman_collection.json`
- **GraphQL Schema:** `schema/graphql_document_schema.py`
- **TypeScript Types:** `graphql_types.ts`

---

## 🎯 What to Test

1. ✅ **Authentication** - Login and get JWT token
2. ✅ **Query Documents** - Get all, filter, search
3. ✅ **Create Document** - Add new documents
4. ✅ **Update Document** - Change status, metadata
5. ✅ **Delete Document** - Soft delete (isActive=false)
6. ✅ **Pagination** - Test limit/offset
7. ✅ **Filters** - Test by type, status, fund, user

---

## 💡 Pro Tips

1. Use **GraphQL Playground** for development
2. Use **Python script** for automated testing
3. Use **Postman** for team collaboration
4. Check **server logs** for debugging
5. Test **error cases** (invalid IDs, missing fields)

---

**Happy Testing! 🎉**

Need help? Check `DOCUMENT_GRAPHQL_TESTING_GUIDE.md` for detailed examples.

