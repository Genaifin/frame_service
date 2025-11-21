# document_api_client.py

import requests
import os
import json

BASE_URL = "https://api.aithonsolutions.com"
LOGIN_URL = f"{BASE_URL}/login"
GRAPHQL_URL = f"{BASE_URL}/v2"

USERNAME = "demo"
PASSWORD = "demo_aithon"


def get_access_token():
    """Logs in to API and returns access token."""
    try:
        payload = {"username": USERNAME, "password": PASSWORD}
        response = requests.post(LOGIN_URL, json=payload)
        response.raise_for_status()

        data = response.json()
        token = data.get("accessToken")
        if not token:
            raise Exception("No accessToken in response.")
        print("✅ Logged in successfully.")
        return token
    except Exception as e:
        print(f"❌ Login failed: {e}")
        return None

def get_all_documents(token):
    """Fetch all documents from the API to check for duplicates."""
    try:
        query = """
        query {
          documents {
            success
            message
            total
            documents {
              id
              name
              path
            }
          }
        }
        """
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        response = requests.post(
            GRAPHQL_URL,
            json={"query": query},
            headers=headers
        )
        response.raise_for_status()

        result = response.json()
        docs = result.get("data", {}).get("documents", {}).get("documents", [])
        existing_docs = {doc["name"] for doc in docs if "name" in doc}

        print(f"📂 Found {len(existing_docs)} existing documents in API.")
        return existing_docs
    except Exception as e:
        print(f"❌ Failed to fetch existing documents: {e}")
        return set()


def create_document(file_path, token):
    """Sends GraphQL mutation to register a downloaded document."""
    try:
        file_name = os.path.basename(file_path)
        size = os.path.getsize(file_path)

        # Basic document type guess — customize later if needed
        doc_type = "CapitalCall" if "capital" in file_name.lower() else "Unknown"

        query = """
        mutation CreateDocument($input: DocumentCreateInput!) {
          createDocument(input: $input) {
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
            }
          }
        }
        """

        variables = {
            "input": {
                "name": file_name,
                "type": doc_type,
                "path": f"/documents/{file_name}",
                "size": size,
                "status": "pending",
                "fundId": 1,
                "replay": False,
                "metadata": {
                    "extractedBy": "frame_engine",
                    "confidence": 0.95
                }
            }
        }

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        response = requests.post(
            GRAPHQL_URL,
            json={"query": query, "variables": variables},
            headers=headers
        )
        response.raise_for_status()

        result = response.json()
        print(f"📄 Uploaded: {file_name}")
        print(json.dumps(result, indent=2))
    except Exception as e:
        print(f"❌ Failed to create document for {file_path}: {e}")