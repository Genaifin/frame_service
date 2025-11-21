# Document Configuration API Documentation

## Overview

The Document Configuration API provides a comprehensive system for managing document type configurations, including their schemas, SLAs, and field definitions. This API allows you to define and manage different document types (such as Capital Call, Distribution, NAV Statement, etc.) along with their JSON schema definitions.

**GraphQL Endpoint**: `http://localhost:8000/v2`

## Table of Contents

1. [Database Schema](#database-schema)
2. [Model Details](#model-details)
3. [GraphQL API](#graphql-api)
4. [Seeder Script](#seeder-script)
5. [Usage Examples](#usage-examples)
6. [Migration Guide](#migration-guide)

---

## Database Schema

### Table: `document_configuration`

The `document_configuration` table stores configuration data for different document types.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `id` | Integer | Primary Key, Auto-increment | Unique identifier |
| `name` | String(255) | Not Null, Indexed | Document type name (e.g., "Capital Call", "Distribution") |
| `description` | Text | Nullable | Long text description of the document type |
| `sla` | Integer | Nullable | Service Level Agreement in days (e.g., 0, 1, 2, 3, 5) |
| `fields` | JSON | Nullable | JSON schema blob defining the document structure |

### Indexes

- Index on `name` column for fast lookups

### Migration

The table is created via Alembic migration:
- **Migration File**: `alembic/versions/b1f0d004108f_add_document_cnfiguration_table.py`
- **Revision ID**: `b1f0d004108f`
- **Down Revision**: `565e6d536ea1`

To apply the migration:
```bash
alembic upgrade head
```

---

## Model Details

### SQLAlchemy Model: `DocumentConfiguration`

**Location**: `database_models.py`

```python
class DocumentConfiguration(Base):
    """Document Configuration model for storing document type schemas and configurations"""
    __tablename__ = 'document_configuration'
    __table_args__ = {'schema': os.getenv('DB_SCHEMA', 'public')}
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False, index=True)
    description = Column(Text, nullable=True)
    sla = Column(Integer, nullable=True)  # SLA in days
    fields = Column(JSON, nullable=True)  # JSON schema blob
```

### Methods

- `to_dict()`: Converts the model instance to a dictionary
- `__repr__()`: String representation for debugging

---

## GraphQL API

### Authentication

All Document Configuration operations require authentication. Include the JWT token in the request headers:

```
Authorization: Bearer YOUR_JWT_TOKEN
```

### Queries

#### 1. `documentConfigurations`

Get all document configurations with optional filtering and pagination.

**Arguments:**
- `filter` (DocumentConfigurationFilterInput, optional): Filter criteria
  - `name` (String, optional): Filter by name (partial match, case-insensitive)
  - `sla` (Int, optional): Filter by SLA days
- `limit` (Int, optional): Maximum number of results (default: 100)
- `offset` (Int, optional): Number of results to skip (default: 0)

**Returns:** `DocumentConfigurationListResponse`

**Example:**
```graphql
query GetDocumentConfigurations($limit: Int, $offset: Int, $filter: DocumentConfigurationFilterInput) {
  documentConfigurations(limit: $limit, offset: $offset, filter: $filter) {
    success
    message
    total
    documentConfigurations {
      id
      name
      description
      sla
      fields
    }
  }
}
```

**Variables:**
```json
{
  "limit": 10,
  "offset": 0,
  "filter": {
    "name": "Capital"
  }
}
```

#### 2. `documentConfiguration`

Get a specific document configuration by ID.

**Arguments:**
- `id` (Int!): Document configuration ID

**Returns:** `DocumentConfigurationResponse`

**Example:**
```graphql
query GetDocumentConfiguration($id: Int!) {
  documentConfiguration(id: $id) {
    success
    message
    documentConfiguration {
      id
      name
      description
      sla
      fields
    }
  }
}
```

**Variables:**
```json
{
  "id": 1
}
```

#### 3. `documentConfigurationByName`

Get a document configuration by name (exact match).

**Arguments:**
- `name` (String!): Document configuration name

**Returns:** `DocumentConfigurationResponse`

**Example:**
```graphql
query GetDocumentConfigurationByName($name: String!) {
  documentConfigurationByName(name: $name) {
    success
    message
    documentConfiguration {
      id
      name
      description
      sla
      fields
    }
  }
}
```

**Variables:**
```json
{
  "name": "Capital Call"
}
```

### Mutations

#### 1. `createDocumentConfiguration`

Create a new document configuration.

**Arguments:**
- `input` (DocumentConfigurationCreateInput!): Configuration data
  - `name` (String!): Document type name
  - `description` (String, optional): Description
  - `sla` (Int, optional): SLA in days
  - `fields` (JSON, optional): JSON schema blob

**Returns:** `DocumentConfigurationResponse`

**Example:**
```graphql
mutation CreateDocumentConfiguration($input: DocumentConfigurationCreateInput!) {
  createDocumentConfiguration(input: $input) {
    success
    message
    documentConfiguration {
      id
      name
      description
      sla
      fields
    }
  }
}
```

**Variables:**
```json
{
  "input": {
    "name": "Private Placement Memorandum (PPM)",
    "description": "A Private Placement Memorandum (PPM), also known as a private offering document and confidential offering memorandum, is a securities disclosure document used in a private offering of securities by a private placement issuer or an investment fund.",
    "sla": 5,
    "fields": {
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "properties": {
        "Fund name and legal entity": {
          "fieldName": "Fundnameandlegalentity",
          "type": "String",
          "description": "Official registered name and legal entity structure",
          "group": "Fund Overview & Basics"
        },
        "Fund type": {
          "fieldName": "Fundtype",
          "type": "String",
          "description": "Classification (e.g., Hedge Fund, PE, VC, Real Estate)",
          "group": "Fund Overview & Basics"
        },
        "Fund manager/General Partner": {
          "fieldName": "Fundmanager/GeneralPartner",
          "type": "String",
          "description": "Entity responsible for managing fund operations",
          "group": "Fund Overview & Basics"
        },
        "Primary investment objective": {
          "fieldName": "Primaryinvestmentobjective",
          "type": "String",
          "description": "Main goal (e.g., capital appreciation, income)",
          "group": "Investment Strategy & Objective"
        },
        "Management fee rate": {
          "fieldName": "Managementfeerate",
          "type": "Numeric",
          "description": "Annual fee as percentage of AUM/commitments",
          "group": "Management Fees"
        },
        "Performance fee/carry rate": {
          "fieldName": "Performancefee/carryrate",
          "type": "Numeric",
          "description": "Percentage of profits paid to manager",
          "group": "Performance Fees/Carried Interest:"
        }
      }
    }
  }
}
```

#### 2. `updateDocumentConfiguration`

Update an existing document configuration.

**Arguments:**
- `id` (Int!): Document configuration ID
- `input` (DocumentConfigurationUpdateInput!): Updated data
  - `name` (String, optional): New name
  - `description` (String, optional): New description
  - `sla` (Int, optional): New SLA
  - `fields` (JSON, optional): New JSON schema

**Returns:** `DocumentConfigurationResponse`

**Example:**
```graphql
mutation UpdateDocumentConfiguration($id: Int!, $input: DocumentConfigurationUpdateInput!) {
  updateDocumentConfiguration(id: $id, input: $input) {
    success
    message
    documentConfiguration {
      id
      name
      description
      sla
      fields
    }
  }
}
```

**Variables:**
```json
{
  "id": 1,
  "input": {
    "sla": 1,
    "description": "Updated description"
  }
}
```

#### 3. `deleteDocumentConfiguration`

Delete a document configuration (hard delete).

**Arguments:**
- `id` (Int!): Document configuration ID

**Returns:** `DocumentConfigurationResponse`

**Example:**
```graphql
mutation DeleteDocumentConfiguration($id: Int!) {
  deleteDocumentConfiguration(id: $id) {
    success
    message
  }
}
```

**Variables:**
```json
{
  "id": 1
}
```

### Response Types

#### `DocumentConfigurationType`

```graphql
type DocumentConfigurationType {
  id: Int!
  name: String!
  description: String
  sla: Int
  fields: JSON
}
```

#### `DocumentConfigurationResponse`

```graphql
type DocumentConfigurationResponse {
  success: Boolean!
  message: String!
  documentConfiguration: DocumentConfigurationType
}
```

#### `DocumentConfigurationListResponse`

```graphql
type DocumentConfigurationListResponse {
  success: Boolean!
  message: String!
  documentConfigurations: [DocumentConfigurationType!]!
  total: Int!
}
```

---

## Seeder Script

### Overview

The seeder script (`seed_document_configurations.py`) populates the `document_configuration` table with predefined document type configurations from JSON files.

### Location

- **Script**: `seed_document_configurations.py`
- **Data Files**: `data/document_configurations/`

### Supported Document Types

The seeder processes the following JSON files:

1. `CapitalCall.json` → "Capital Call"
2. `Distributions.json` → "Distribution"
3. `NAVStatement.json` → "NAVStatement"
4. `Brokerage_Statement.json` → "Brokerage Statement"
5. `k-1_,_1065_(_for_a_partnership).json` → "k-1 , 1065 ( for a partnership)"
6. `Private_Placement_Memorandum_(PPM).json` → "Private Placement Memorandum (PPM)"

### JSON File Structure

Each JSON file should follow this structure:

```json
{
  "response": [
    {
      "document_type": "Capital Call",
      "description": "A capital call notice for funds...",
      "sla": "T + 0 Days",
      "schema_blob": {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
          "Account": {
            "type": "string",
            "description": "..."
          }
        }
      }
    }
  ]
}
```

### Running the Seeder

**Prerequisites:**
1. Database migration must be applied (`alembic upgrade head`)
2. JSON files must be present in `data/document_configurations/`

**Command:**
```bash
python seed_document_configurations.py
```

**Output:**
The script will:
- Process each JSON file
- Parse SLA strings (e.g., "T + 0 Days" → 0)
- Extract document type, description, SLA, and schema blob
- Insert new configurations or update existing ones (based on name)
- Display a summary of inserted/updated/skipped records

**Example Output:**
```
INFO: Processing file: CapitalCall.json
INFO: Inserted document configuration: Capital Call
INFO: Processing file: Distributions.json
INFO: Inserted document configuration: Distribution
...
============================================================
Document Configuration Seeding Summary:
  Inserted: 6
  Updated: 0
  Skipped: 0
============================================================
```

### SLA Parsing

The seeder automatically parses SLA strings to integers:
- `"T + 0 Days"` → `0`
- `"T + 1 Days"` → `1`
- `"T + 2 Days"` → `2`
- `"T + 3 Days"` → `3`
- `"T + 5 Days"` → `5`

---

## Usage Examples

### Complete Workflow Example

#### Step 1: Apply Migration

```bash
alembic upgrade head
```

#### Step 2: Seed Initial Data

```bash
python seed_document_configurations.py
```

#### Step 3: Query Document Configurations

```graphql
query {
  documentConfigurations(limit: 10) {
    success
    message
    total
    documentConfigurations {
      id
      name
      description
      sla
    }
  }
}
```

#### Step 4: Get Specific Configuration

```graphql
query {
  documentConfigurationByName(name: "Capital Call") {
    success
    message
    documentConfiguration {
      id
      name
      sla
      fields
    }
  }
}
```

#### Step 5: Create Custom Configuration

```graphql
mutation {
  createDocumentConfiguration(input: {
    name: "Custom Document Type"
    description: "Custom document type description"
    sla: 2
    fields: {
      "$schema": "http://json-schema.org/draft-07/schema#",
      "type": "object",
      "properties": {
        "Field1": {
          "type": "string"
        }
      }
    }
  }) {
    success
    message
    documentConfiguration {
      id
      name
    }
  }
}
```

### Filtering Example

```graphql
query {
  documentConfigurations(
    filter: {
      sla: 0
    }
    limit: 5
  ) {
    success
    total
    documentConfigurations {
      id
      name
      sla
    }
  }
}
```

### Update Example

```graphql
mutation {
  updateDocumentConfiguration(
    id: 1
    input: {
      sla: 1
      description: "Updated description with new SLA"
    }
  ) {
    success
    message
    documentConfiguration {
      id
      name
      sla
      description
    }
  }
}
```

---

## Migration Guide

### Creating the Table

1. **Run the migration:**
   ```bash
   alembic upgrade head
   ```

2. **Verify the table exists:**
   ```sql
   SELECT * FROM public.document_configuration;
   ```

### Rolling Back

If you need to rollback the migration:

```bash
alembic downgrade -1
```

This will drop the `document_configuration` table and its index.

---

## Error Handling

### Common Errors

#### 1. Duplicate Name Error

**Error:** `Document configuration with name 'X' already exists`

**Solution:** Use `updateDocumentConfiguration` mutation instead, or delete the existing configuration first.

#### 2. Not Found Error

**Error:** `Document configuration with ID X not found`

**Solution:** Verify the ID exists using the `documentConfigurations` query.

#### 3. Authentication Error

**Error:** `Authentication required`

**Solution:** Ensure you include a valid JWT token in the `Authorization` header.

### Error Response Format

All errors follow this format:

```json
{
  "success": false,
  "message": "Error description",
  "documentConfiguration": null
}
```

---

## Best Practices

1. **Naming Convention**: Use consistent naming for document types (e.g., "Capital Call" not "capital call" or "CapitalCall")

2. **SLA Values**: Store SLA as integer days (0, 1, 2, etc.) for easy filtering and comparison

3. **JSON Schema**: Ensure `fields` contains valid JSON schema following JSON Schema Draft 7 specification

4. **Descriptions**: Provide clear, descriptive text in the `description` field for better documentation

5. **Validation**: Always validate JSON schema before inserting/updating

6. **Backup**: Backup the `document_configuration` table before bulk updates

---

## Testing

### Using GraphQL Playground

1. Start the server:
   ```bash
   python start_server.py
   ```

2. Navigate to GraphQL Playground:
   ```
   http://localhost:8000/v2
   ```

3. Add authentication header:
   ```
   {
     "Authorization": "Bearer YOUR_JWT_TOKEN"
   }
   ```

4. Test queries and mutations using the examples above.

### Using cURL

```bash
curl -X POST http://localhost:8000/v2 \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -d '{
    "query": "query { documentConfigurations(limit: 10) { success total documentConfigurations { id name sla } } }"
  }'
```

---

## Related Documentation

- [GraphQL API Documentation](./GRAPHQL_API_DOCUMENTATION.md)
- [Document GraphQL Testing Guide](./DOCUMENT_GRAPHQL_TESTING_GUIDE.md)
- [GraphQL Authentication Guide](./GRAPHQL_AUTHENTICATION_GUIDE.md)

---

## Support

For issues or questions:
1. Check the error messages in the GraphQL response
2. Review the server logs for detailed error information
3. Verify database migration status
4. Ensure authentication token is valid

---

**Last Updated**: 2025-01-27

