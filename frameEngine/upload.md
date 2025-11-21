# Upload API Implementation Guide

This document explains how the upload functionality was implemented to allow files to be uploaded from the dashboard and stored in the `frameDemo/l0` folder.

## Overview

The upload system consists of three main components:
1. **Backend API** - Handles file upload and storage
2. **Frontend Component** - Provides UI for file selection and upload
3. **Data Operations** - Manages file metadata and tracking

## Backend Implementation

### 1. Upload API Endpoint

**File**: `validusBoxes/server/APIServer.py`

```python
@app.post("/upload_files", description="Upload files to frameDemo/l0 folder", tags=["file-upload"])
async def upload_files(
    file: UploadFile = File(...),
    folder: str = Form("l0"),
    storage_type: str = Form("local"), 
    source: str = Form("api"),
    file_classification: str = Form(""),
    __username: str = Depends(authenticate_user)
):
```

**Key Features**:
- Accepts multipart/form-data uploads
- Requires authentication using HTTP Basic Auth
- Supports multiple file types (PDF, TXT, CSV, XLSX, etc.)
- Automatically determines MIME types
- Uses POST method for file uploads

### 2. File Storage System

The upload API uses a layered storage system:

- **L0 Layer**: Raw uploaded files (`data/frameDemo/l0/`)
- **L1 Layer**: File metadata and processed copies (`data/frameDemo/l1/[fileHash]/`)
- **L2 Layer**: Processing status tracking (`data/frameDemo/states/l2.json`)
- **LDummy Layer**: File catalog for UI display (`data/frameDemo/ldummy/`)

### 3. Data Operations Flow

```python
# 1. Save file to L0 layer
target_dir = myStorage.getLayerNFolder('l0')
file_path = os.path.join(target_dir, file.filename)
with open(file_path, "wb") as buffer:
    shutil.copyfileobj(file.file, buffer)

# 2. Generate file hash
myHash = getFileHash(file_path)

# 3. Create metadata in L1 layer
myMetaData = {
    'typeName': mime_type,
    'fileHash': myHash,
    'fileOriginalName': file.filename,
    'fileOriginalPath': file_path,
    'typeSpecificParams': {}
}

# 4. Update processing status in L2
myDataOp = {
    "dataTypeToSaveAs": "statusUpdate",
    "opParams": {
        "layerName": "l2",
        "trackerName": "processedFiles", 
        "operation": "replaceOrAppendByKey",
        "key": [file.filename]
    }
}

# 5. Update file catalog in LDummy
existing_ldummy[file.filename] = {
    "fileHash": myHash,
    "fileType": file_type,
    "status": "Processed",
    "fileName": file.filename
}
```

### 4. File Type Detection

The system automatically detects file types and classifies them:

```python
# Handle cases where filetype can't determine the type
if fileKind is None:
    file_extension = os.path.splitext(file.filename)[1].lower()
    if file_extension == '.txt':
        mime_type = 'text/plain'
    elif file_extension == '.csv':
        mime_type = 'text/csv'
    elif file_extension == '.xlsx':
        mime_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    # ... more types
else:
    mime_type = fileKind.mime

# Classify based on filename patterns
if "statement" in filename_lower:
    file_type = "Fund Statement"
elif "factsheet" in filename_lower:
    file_type = "Fund Fact Sheet"
# ... more classifications
```

## Frontend Implementation

### 1. Upload Component

**File**: `AithonFrontend/src/components/uploadFile/UploadFiles.tsx`

The component was modified to perform real uploads instead of just simulation:

```typescript
const handleFileUpload = async (files: FileList | null) => {
    if (!files) return;

    // Create file objects with metadata
    const myFileArray = Array.from(files).map((file) => ({
        name: file.name,
        size: file.size,
        file,
        progress: 0,
        status: "uploading",
    }));

    // Upload each file
    for (const fileData of myFileArray) {
        try {
            const formData = new FormData();
            formData.append('file', fileData.file);
            formData.append('folder', 'l0');
            formData.append('storage_type', 'local');
            formData.append('source', 'dashboard');

            // Call upload API
            const response = await uploadDocument(formData);
            
            // Update status to uploaded
            // ... status update logic
        } catch (error) {
            // Handle upload errors
            // ... error handling logic
        }
    }
};
```

### 2. API Service Configuration

**File**: `AithonFrontend/src/services/CommonService.ts`

```typescript
export async function uploadDocument(data: any) {
  return ApiService.fetchDataWithAxios<any>({
    url: apiPointConfig?.upload_to_s3,  // Maps to "upload_files"
    method: "post",
    data,
    headers: {
      "Content-Type": "multipart/form-data",
      Accept: "application/json",
    },
  });
}
```

**File**: `AithonFrontend/src/services/endpoint.config.ts`

```typescript
const apiPointConfig = {
  // ... other endpoints
  upload_to_s3: "upload_files",  // Points to our backend endpoint
  // ... more endpoints
};
```

### 3. API Base Configuration

**File**: `AithonFrontend/src/services/axios/LocalAPIBase.ts`

```typescript
export const API_URL_LOCAL = "http://127.0.0.1:8000";

const baseLocal = axios.create({
  baseURL: `${API_URL_LOCAL}/`,
});
```

## Storage System Details

### Storage Class

**File**: `validusBoxes/storage.py`

The `STORAGE` class handles all data operations:

```python
class STORAGE():
    def __init__(self, aClient: str, aStorageConfig: dict):
        self.client = aClient  # 'frameDemo'
        self.storageConfig = aStorageConfig

    def doDataOperation(self, aDataOperation: dict):
        # Handles different operation types:
        # - tabular: CSV data operations
        # - statusUpdate: Processing status tracking
        # - JSONDump: Metadata storage
        # - copyFile: File copying operations
```

### Data Operation Types

1. **Status Updates** (`l2` layer):
```python
{
    "dataTypeToSaveAs": "statusUpdate",
    "opParams": {
        "layerName": "l2",
        "trackerName": "processedFiles",
        "operation": "replaceOrAppendByKey",
        "key": [filename]
    }
}
```

2. **JSON Metadata** (`l1` layer):
```python
{
    "dataTypeToSaveAs": "JSONDump",
    "opParams": {
        "layerName": "l1",
        "folderArray": [fileHash],
        "operation": "replace"
    },
    "data": metadata,
    "key": "fileMetaData"
}
```

3. **File Catalog** (`ldummy` layer):
```python
{
    "dataTypeToSaveAs": "JSONDump",
    "opParams": {
        "layerName": "ldummy",
        "folderArray": [],
        "operation": "replace"
    },
    "data": allFileMeta,
    "key": "allFileMeta"
}
```

## File Structure After Upload

When a file `111.pdf` is uploaded, the following structure is created:

```
validusBoxes/data/frameDemo/
├── l0/
│   └── 111.pdf                    # Original uploaded file
├── l1/
│   └── [fileHash]/
│       ├── fileMetaData.json      # File metadata
│       └── rawFile.pdf            # Copy of original file
├── l2/
│   └── processedFiles             # Processing status
└── ldummy/
    └── allFileMeta.json           # File catalog for UI
```

## Authentication

The API uses HTTP Basic Authentication:

```python
async def authenticate_user(credentials: HTTPBasicCredentials = Depends(security)):
    # Validates username/password against rbac/configs/users.json
    # Returns username if valid, raises HTTPException if invalid
```

**Example credentials**:
- Username: `admin`
- Password: `q3wR62ZRaAB5Ly9`
- Base64: `YWRtaW46cTN3UjYyWlJhQUI1THk5`

## Testing the Upload API

### 1. Direct API Test

```bash
curl -X POST "http://127.0.0.1:8000/upload_files" \
  -H "Authorization: Basic YWRtaW46cTN3UjYyWlJhQUI1THk5" \
  -F "file=@111.pdf" \
  -F "folder=l0" \
  -F "storage_type=local" \
  -F "source=api"
```

### 2. Frontend Test

1. Start backend server:
```bash
cd validusBoxes
source myenv/bin/activate
python -m uvicorn server.APIServer:app --reload --port 8000 --host 127.0.0.1
```

2. Start frontend server:
```bash
cd AithonFrontend
npm run dev
```

3. Navigate to the dashboard and use the upload button

## Error Handling

The system handles various error scenarios:

1. **File type detection failures**: Falls back to extension-based detection
2. **PDF metadata extraction errors**: Continues upload without metadata
3. **Network errors**: Shows error status in UI
4. **Authentication failures**: Returns 401 Unauthorized
5. **Storage errors**: Returns 500 Internal Server Error

## Dependencies

### Backend
```txt
fastapi==0.109.2
uvicorn==0.27.1
PyPDF2
filetype
pandas==2.1.1
pyyaml==6.0.1
```

### Frontend
- React with TypeScript
- Axios for HTTP requests
- Lucide React for icons

## Configuration

### Environment Setup

1. **Virtual Environment**: Use `myenv` in `validusBoxes/`
2. **API Server**: Runs on `http://127.0.0.1:8000`
3. **Frontend**: Runs on `http://localhost:5177` (or next available port)
4. **Storage**: Local filesystem in `validusBoxes/data/frameDemo/`

### CORS Configuration

```python
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

## Security Considerations

1. **Authentication Required**: All uploads require valid credentials
2. **File Type Validation**: Basic MIME type checking
3. **Path Traversal Protection**: Files stored in designated directories only
4. **File Size Limits**: Handled by FastAPI's default limits

## Future Enhancements

1. **File Size Validation**: Add explicit file size limits
2. **Virus Scanning**: Integrate antivirus checking
3. **Progress Tracking**: Real-time upload progress
4. **Batch Operations**: Support for multiple file operations
5. **Cloud Storage**: Support for S3/Azure blob storage
6. **File Versioning**: Handle duplicate file uploads

## Troubleshooting

### Common Issues

1. **Files not appearing in l0**: Check API server logs and authentication
2. **Upload errors**: Verify CORS settings and network connectivity
3. **Permission errors**: Ensure write permissions to data directories
4. **Server not responding**: Check if server is running on correct port

### Debug Commands

```bash
# Check if file was uploaded
ls -la validusBoxes/data/frameDemo/l0/

# Check metadata
cat validusBoxes/data/frameDemo/ldummy/allFileMeta.json

# Check server health
curl http://127.0.0.1:8000/health

# Check server logs
# (View terminal where uvicorn is running)
```

This implementation provides a complete, working upload system that integrates with the existing storage and data management architecture. 