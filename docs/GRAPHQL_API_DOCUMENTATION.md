# Complete GraphQL API Documentation

Generated on: 2025-01-27

## Overview

This document provides comprehensive documentation for the complete GraphQL API implementation. The API supports user management, client management, and role management operations with pure data responses.

**GraphQL v2 Endpoint**: `http://localhost:8000/v2`

## Key Features

- ✅ **Pure Data Responses**: No response formatting or structure wrapping
- ✅ **Complete CRUD Operations**: Create, Read, Update, Delete for all entities
- ✅ **Advanced Filtering**: Search, status filtering, pagination
- ✅ **Relationship Loading**: Automatic loading of related data
- ✅ **Bulk Operations**: Efficient multi-record operations
- ✅ **Type Safety**: Full TypeScript support available

## Quick Start

### 1. GraphQL Playground
Visit `http://localhost:8000/v2` for interactive exploration with:
- Schema explorer
- Query builder with auto-completion
- Built-in examples
- Real-time testing

### 2. Authentication
All operations require authentication headers.

## User Management Operations

### Queries

#### `users`
Get users with advanced filtering and pagination.

**Arguments:**
- `id` (Int): Get specific wf by ID
- `search` (String): Search by name, email, or username
- `status_filter` (String): Filter by 'active', 'inactive', or None for all
- `limit` (Int): Number of users to return (default: 10)
- `offset` (Int): Number of users to skip (default: 0)

**Example:**
```graphql
query GetUsers($limit: Int, $offset: Int, $search: String) {
  users(limit: $limit, offset: $offset, search: $search) {
    id
    username
    email
    displayName
    firstName
    lastName
    isActive
    role {
      roleName
      roleCode
    }
    client {
      name
      code
    }
  }
}
```

#### `userEditForm`
Get user data specifically for edit forms.

**Arguments:**
- `user_id` (Int!): User ID

**Example:**
```graphql
query GetUserEditForm($userId: Int!) {
  userEditForm(userId: $userId) {
    id
    username
    email
    displayName
    firstName
    lastName
    isActive
    role {
      roleName
      roleCode
    }
    client {
      name
      code
    }
  }
}
```

### Mutations

#### `createUser`
Create a new user.

**Arguments:**
- `input` (UserCreateInput!): User creation data

**Example:**
```graphql
mutation CreateUser($input: UserCreateInput!) {
  createUser(input: $input) {
    id
    username
    email
    displayName
    firstName
    lastName
    isActive
  }
}
```

**Variables:**
```json
{
  "input": {
    "firstName": "John",
    "lastName": "Doe",
    "email": "john.doe@example.com",
    "jobTitle": "Software Engineer",
    "roleId": 1,
    "clientId": 1
  }
}
```

#### `updateUser`
Update an existing user.

**Arguments:**
- `user_id` (Int!): User ID to update
- `input` (UserUpdateInput!): Update data

**Example:**
```graphql
mutation UpdateUser($userId: Int!, $input: UserUpdateInput!) {
  updateUser(userId: $userId, input: $input) {
    id
    displayName
    firstName
    lastName
    isActive
  }
}
```

#### `deleteUser`
Delete a user (soft delete).

**Arguments:**
- `user_id` (Int!): User ID to delete

**Example:**
```graphql
mutation DeleteUser($userId: Int!) {
  deleteUser(userId: $userId)
}
```

#### `toggleUserStatus`
Toggle user active/inactive status.

**Arguments:**
- `user_id` (Int!): User ID

**Example:**
```graphql
mutation ToggleUserStatus($userId: Int!) {
  toggleUserStatus(userId: $userId) {
    id
    displayName
    isActive
  }
}
```

#### `createUsersBulk`
Create multiple users from forms.

**Arguments:**
- `input` (BulkUserCreateInput!): Bulk creation data

**Example:**
```graphql
mutation CreateUsersBulk($input: BulkUserCreateInput!) {
  createUsersBulk(input: $input) {
    id
    username
    displayName
    firstName
    lastName
    email
  }
}
```

## Client Management Operations

### Queries

#### `clients`
Get clients with filtering and pagination.

**Arguments:**
- `id` (Int): Get specific client by ID
- `search` (String): Search by name, code, or description
- `status_filter` (String): Filter by 'active', 'inactive', or None for all
- `limit` (Int): Number of clients to return (default: 10)
- `offset` (Int): Number of clients to skip (default: 0)

**Example:**
```graphql
query GetClients($limit: Int, $offset: Int, $search: String) {
  clients(limit: $limit, offset: $offset, search: $search) {
    id
    name
    code
    description
    type
    isActive
    createdAt
  }
}
```

#### `clientDetails`
Get detailed client information including users.

**Arguments:**
- `client_id` (Int!): Client ID

**Example:**
```graphql
query GetClientDetails($clientId: Int!) {
  clientDetails(clientId: $clientId) {
    id
    name
    code
    description
    type
    userCount
    users {
      id
      displayName
      email
    }
  }
}
```

### Mutations

#### `createClient`
Create a new client.

**Arguments:**
- `input` (ClientCreateInput!): Client creation data

**Example:**
```graphql
mutation CreateClient($input: ClientCreateInput!) {
  createClient(input: $input) {
    id
    name
    code
    description
    type
    isActive
  }
}
```

#### `updateClient`
Update an existing client.

**Arguments:**
- `client_id` (Int!): Client ID to update
- `input` (ClientUpdateInput!): Update data

**Example:**
```graphql
mutation UpdateClient($clientId: Int!, $input: ClientUpdateInput!) {
  updateClient(clientId: $clientId, input: $input) {
    id
    name
    description
    isActive
  }
}
```

#### `deleteClient`
Delete a client (soft delete).

**Arguments:**
- `client_id` (Int!): Client ID to delete
**Example:**
```graphql
mutation DeleteClient($clientId: Int!) {
  deleteClient(clientId: $clientId)
}
```

#### `toggleClientStatus`
Toggle client active/inactive status.

**Arguments:**
- `client_id` (Int!): Client ID

**Example:**
```graphql
mutation ToggleClientStatus($clientId: Int!) {
  toggleClientStatus(clientId: $clientId) {
    id
    name
    isActive
  }
}
```

## Role Management Operations

### Queries

#### `roles`
Get roles with filtering and pagination.

**Arguments:**
- `id` (Int): Get specific role by ID
- `search` (String): Search by role name, code, or description
- `status_filter` (String): Filter by 'active', 'inactive', or None for all
- `limit` (Int): Number of roles to return (default: 10)
- `offset` (Int): Number of roles to skip (default: 0)

**Example:**
```graphql
query GetRoles($limit: Int, $offset: Int, $search: String) {
  roles(limit: $limit, offset: $offset, search: $search) {
    id
    roleName
    roleCode
    description
    isActive
    createdAt
  }
}
```

#### `roleDetails`
Get detailed role information including users and permissions.

**Arguments:**
- `role_id` (Int!): Role ID

**Example:**
```graphql
query GetRoleDetails($roleId: Int!) {
  roleDetails(roleId: $roleId) {
    id
    roleName
    roleCode
    description
    isActive
    userCount
    users {
      id
      displayName
      email
    }
    modulePermissions
  }
}
```

#### `rolesSummary`
Get aggregated role data with user counts.

**Arguments:**
- `page` (Int): Page number (default: 1)
- `page_size` (Int): Page size (default: 10)

**Example:**
```graphql
query GetRolesSummary($page: Int, $pageSize: Int) {
  rolesSummary(page: $page, pageSize: $pageSize) {
    roleId
    roleName
    roleCode
    totalUsers
    products
    status
    isActive
  }
}
```

### Mutations

#### `createRole`
Create a new role with permissions.

**Arguments:**
- `input` (RoleCreateInput!): Role creation data

**Example:**
```graphql
mutation CreateRole($input: RoleCreateInput!) {
  createRole(input: $input) {
    id
    roleName
    roleCode
    description
    isActive
  }
}
```

**Variables:**
```json
{
  "input": {
    "roleName": "Auditor",
    "roleCode": "auditor",
    "description": "Auditor role with limited permissions",
    "isActive": true,
    "permissions": [
      {
        "module": "Frame",
        "create": true,
        "view": true,
        "update": false,
        "delete": false
      }
    ]
  }
}
```

#### `updateRole`
Update an existing role with permissions.

**Arguments:**
- `role_id` (Int!): Role ID to update
- `input` (RoleUpdateInput!): Update data

**Example:**
```graphql
mutation UpdateRole($roleId: Int!, $input: RoleUpdateInput!) {
  updateRole(roleId: $roleId, input: $input) {
    id
    roleName
    description
    isActive
  }
}
```

#### `deleteRole`
Delete a role (soft delete by default).

**Arguments:**
- `role_id` (Int!): Role ID to delete
- `hard_delete` (Boolean): If true, permanently delete (default: false)

**Example:**
```graphql
mutation DeleteRole($roleId: Int!, $hardDelete: Boolean) {
  deleteRole(roleId: $roleId, hardDelete: $hardDelete)
}
```

#### `activateRole`
Activate a role.

**Arguments:**
- `role_id` (Int!): Role ID

**Example:**
```graphql
mutation ActivateRole($roleId: Int!) {
  activateRole(roleId: $roleId) {
    id
    roleName
    isActive
  }
}
```

#### `inactivateRoleWithReassignment`
Inactivate a role and reassign users to new roles.

**Arguments:**
- `input` (RoleInactivationInput!): Inactivation data

**Example:**
```graphql
mutation InactivateRoleWithReassignment($input: RoleInactivationInput!) {
  inactivateRoleWithReassignment(input: $input)
}
```

## Fund Manager Operations

### Queries

#### `fundManagers`
Get fund managers with filtering and pagination.

**Parameters:**
- `id` (Int): Get specific fund manager by ID
- `search` (String): Search in name, first name, last name, email
- `status_filter` (String): Filter by status ('active' or 'inactive')
- `limit` (Int): Number of results (default: 10)
- `offset` (Int): Pagination offset (default: 0)

**Example:**
```graphql
query GetFundManagers($search: String, $statusFilter: String, $limit: Int, $offset: Int) {
  fundManagers(search: $search, statusFilter: $statusFilter, limit: $limit, offset: $offset) {
    id
    fundManagerName
    contactTitle
    contactFirstName
    contactLastName
    contactEmail
    contactNumber
    status
    createdAt
    updatedAt
  }
}
```

**Variables:**
```json
{
  "search": "test",
  "statusFilter": "active",
  "limit": 10,
  "offset": 0
}
```

### Mutations

#### `createFundManager`
Create a new fund manager.

**Parameters:**
- `input` (FundManagerCreateInput!): Fund manager creation data

**Example:**
```graphql
mutation CreateFundManager($input: FundManagerCreateInput!) {
  createFundManager(input: $input) {
    id
    fundManagerName
    contactTitle
    contactFirstName
    contactLastName
    contactEmail
    contactNumber
    status
    createdAt
  }
}
```

**Variables:**
```json
{
  "input": {
    "fundManagerName": "Test Fund Manager",
    "title": "Mr.",
    "firstName": "John",
    "lastName": "Doe",
    "email": "john.doe@testfund.com",
    "contactNumber": "+1-555-0123"
  }
}
```

#### `updateFundManager`
Update an existing fund manager.

**Parameters:**
- `fund_manager_id` (Int!): Fund manager ID to update
- `input` (FundManagerUpdateInput!): Update data

**Example:**
```graphql
mutation UpdateFundManager($fundManagerId: Int!, $input: FundManagerUpdateInput!) {
  updateFundManager(fundManagerId: $fundManagerId, input: $input) {
    id
    fundManagerName
    contactTitle
    contactFirstName
    contactLastName
    contactEmail
    contactNumber
    status
    updatedAt
  }
}
```

**Variables:**
```json
{
  "fundManagerId": 1,
  "input": {
    "title": "Dr.",
    "contactNumber": "+1-555-9999"
  }
}
```

#### `toggleFundManagerStatus`
Toggle fund manager active/inactive status.

**Parameters:**
- `fund_manager_id` (Int!): Fund manager ID

**Example:**
```graphql
mutation ToggleFundManagerStatus($fundManagerId: Int!) {
  toggleFundManagerStatus(fundManagerId: $fundManagerId) {
    id
    fundManagerName
    status
    updatedAt
  }
}
```

**Variables:**
```json
{
  "fundManagerId": 1
}
```

#### `deleteFundManager`
Delete a fund manager (soft delete by setting status to inactive).

**Parameters:**
- `fund_manager_id` (Int!): Fund manager ID to delete

**Example:**
```graphql
mutation DeleteFundManager($fundManagerId: Int!) {
  deleteFundManager(fundManagerId: $fundManagerId)
}
```

**Variables:**
```json
{
  "fundManagerId": 1
}
```

## Input Types

### UserCreateInput
```graphql
input UserCreateInput {
  firstName: String!
  lastName: String!
  email: String!
  jobTitle: String!
  roleId: Int!
  clientId: Int
}
```

### UserUpdateInput
```graphql
input UserUpdateInput {
  firstName: String
  lastName: String
  email: String
  jobTitle: String
  roleId: Int
  clientId: Int
  isActive: Boolean
}
```

### ClientCreateInput
```graphql
input ClientCreateInput {
  name: String!
  code: String
  description: String
  type: String
  contactTitle: String
  contactFirstName: String
  contactLastName: String
  contactEmail: String
  contactNumber: String
  adminTitle: String
  adminFirstName: String
  adminLastName: String
  adminEmail: String
  adminJobTitle: String
  isActive: Boolean
}
```

### RoleCreateInput
```graphql
input RoleCreateInput {
  roleName: String!
  roleCode: String!
  description: String
  isActive: Boolean
  permissions: [JSON]
}
```

### FundManagerCreateInput
```graphql
input FundManagerCreateInput {
  fundManagerName: String!
  title: String
  firstName: String!
  lastName: String!
  email: String!
  contactNumber: String
}
```

### FundManagerUpdateInput
```graphql
input FundManagerUpdateInput {
  fundManagerName: String
  title: String
  firstName: String
  lastName: String
  email: String
  contactNumber: String
  status: String
}
```

## Error Handling

All operations return structured error responses:

```json
{
  "data": null,
  "errors": [
    {
      "message": "Error description",
      "locations": [{"line": 1, "column": 1}],
      "path": ["operationName"]
    }
  ]
}
```

## Frontend Integration

### Apollo Client (React)
```javascript
import { ApolloClient, InMemoryCache, gql } from '@apollo/client';

const client = new ApolloClient({
  uri: 'http://localhost:8000/v2',
  cache: new InMemoryCache()
});

const GET_USERS = gql`
  query GetUsers($limit: Int, $offset: Int) {
    users(limit: $limit, offset: $offset) {
      id
      username
      displayName
      role {
        roleName
      }
    }
  }
`;
```

### Vanilla JavaScript
```javascript
const query = `
  query GetUsers($limit: Int, $offset: Int) {
    users(limit: $limit, offset: $offset) {
      id
      username
      displayName
    }
  }
`;

const response = await fetch('http://localhost:8000/v2', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    query,
    variables: { limit: 10, offset: 0 }
  })
});

const data = await response.json();
```

## Testing

Run the comprehensive test script:
```bash
python test_complete_graphql.py
```

## Best Practices

1. **Use Variables**: Always use variables for dynamic values
2. **Request Only Needed Fields**: GraphQL allows you to request only the fields you need
3. **Handle Errors**: Always check for errors in the response
4. **Use Pagination**: For large datasets, use pagination parameters
5. **Cache Results**: Implement appropriate caching strategies
6. **Pure Data**: All responses return pure data without wrapper structures

## Summary

The GraphQL API now provides **25+ operations** covering:

### **User Management (7 operations)**
- `users` - Advanced user listing with filtering
- `userEditForm` - User edit form data
- `createUser` - Create single user
- `updateUser` - Update user
- `deleteUser` - Delete user (soft delete)
- `toggleUserStatus` - Toggle user status
- `createUsersBulk` - Bulk user creation

### **Client Management (6 operations)**
- `clients` - Client listing with filtering
- `clientDetails` - Detailed client information
- `createClient` - Create client
- `updateClient` - Update client
- `deleteClient` - Delete client (soft delete)
- `toggleClientStatus` - Toggle client status

### **Role Management (8 operations)**
- `roles` - Role listing with filtering
- `roleDetails` - Detailed role information
- `rolesSummary` - Aggregated role data
- `createRole` - Create role with permissions
- `updateRole` - Update role with permissions
- `deleteRole` - Delete role (soft/hard delete)
- `activateRole` - Activate role
- `inactivateRoleWithReassignment` - Inactivate role with user reassignment

### **Fund Manager Management (5 operations)**
- `fundManagers` - Fund manager listing with search and filtering
- `createFundManager` - Create new fund manager
- `updateFundManager` - Update fund manager details
- `toggleFundManagerStatus` - Toggle active/inactive status
- `deleteFundManager` - Soft delete fund manager

All operations provide **pure data responses** without any response structure formatting, giving frontend developers complete flexibility in how they handle and display the data.

---

**Happy coding! 🚀**
