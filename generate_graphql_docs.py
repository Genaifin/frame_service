#!/usr/bin/env python3
"""
GraphQL API Documentation Generator
Generates comprehensive documentation for frontend team
"""

import requests
import json
from typing import Dict, Any, List
from datetime import datetime

class GraphQLDocumentationGenerator:
    """Generate comprehensive GraphQL API documentation"""
    
    def __init__(self, graphql_url: str = "http://localhost:8000/graphql"):
        self.graphql_url = graphql_url
        self.schema = None
        self.operations = {}
    
    def introspect_schema(self) -> Dict[str, Any]:
        """Get complete schema information via introspection"""
        introspection_query = """
        query IntrospectionQuery {
            __schema {
                queryType { name }
                mutationType { name }
                subscriptionType { name }
                types {
                    ...FullType
                }
                directives {
                    name
                    description
                    locations
                    args {
                        ...InputValue
                    }
                }
            }
        }
        
        fragment FullType on __Type {
            kind
            name
            description
            fields(includeDeprecated: true) {
                name
                description
                args {
                    ...InputValue
                }
                type {
                    ...TypeRef
                }
                isDeprecated
                deprecationReason
            }
            inputFields {
                ...InputValue
            }
            interfaces {
                ...TypeRef
            }
            enumValues(includeDeprecated: true) {
                name
                description
                isDeprecated
                deprecationReason
            }
            possibleTypes {
                ...TypeRef
            }
        }
        
        fragment InputValue on __InputValue {
            name
            description
            type { ...TypeRef }
            defaultValue
        }
        
        fragment TypeRef on __Type {
            kind
            name
            ofType {
                kind
                name
                ofType {
                    kind
                    name
                    ofType {
                        kind
                        name
                        ofType {
                            kind
                            name
                            ofType {
                                kind
                                name
                                ofType {
                                    kind
                                    name
                                    ofType {
                                        kind
                                        name
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        """
        
        try:
            response = requests.post(
                self.graphql_url,
                json={"query": introspection_query},
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code == 200:
                data = response.json()
                if "data" in data:
                    self.schema = data["data"]["__schema"]
                    return self.schema
                else:
                    print(f"Error in introspection: {data}")
                    return {}
            else:
                print(f"HTTP Error: {response.status_code}")
                return {}
                
        except Exception as e:
            print(f"Exception during introspection: {e}")
            return {}
    
    def generate_operations_documentation(self) -> Dict[str, Any]:
        """Generate documentation for all operations"""
        if not self.schema:
            self.introspect_schema()
        
        operations = {
            "queries": {},
            "mutations": {},
            "types": {},
            "examples": {}
        }
        
        # Process types
        for type_info in self.schema.get("types", []):
            if type_info["name"].startswith("__"):
                continue
                
            operations["types"][type_info["name"]] = {
                "kind": type_info["kind"],
                "description": type_info.get("description", ""),
                "fields": []
            }
            
            if type_info.get("fields"):
                for field in type_info["fields"]:
                    operations["types"][type_info["name"]]["fields"].append({
                        "name": field["name"],
                        "description": field.get("description", ""),
                        "type": self._resolve_type(field["type"]),
                        "args": [self._format_input_value(arg) for arg in field.get("args", [])]
                    })
        
        # Process queries and mutations
        query_type = self.schema.get("queryType", {}).get("name")
        mutation_type = self.schema.get("mutationType", {}).get("name")
        
        if query_type and query_type in operations["types"]:
            operations["queries"] = operations["types"][query_type]["fields"]
        
        if mutation_type and mutation_type in operations["types"]:
            operations["mutations"] = operations["types"][mutation_type]["fields"]
        
        # Generate examples
        operations["examples"] = self._generate_examples()
        
        return operations
    
    def _resolve_type(self, type_info: Dict[str, Any]) -> str:
        """Resolve GraphQL type to string representation"""
        if type_info["kind"] == "NON_NULL":
            return f"{self._resolve_type(type_info['ofType'])}!"
        elif type_info["kind"] == "LIST":
            return f"[{self._resolve_type(type_info['ofType'])}]"
        else:
            # Map backend type names to frontend type names if needed
            type_name = type_info["name"]
            if type_name == "RoleOrClientBasedModuleLevelPermission":
                return "RoleModulePermission"
            return type_name
    
    def _format_input_value(self, input_value: Dict[str, Any]) -> Dict[str, Any]:
        """Format input value for documentation"""
        return {
            "name": input_value["name"],
            "description": input_value.get("description", ""),
            "type": self._resolve_type(input_value["type"]),
            "defaultValue": input_value.get("defaultValue")
        }
    
    def _generate_examples(self) -> Dict[str, Any]:
        """Generate example queries and mutations"""
        return {
            "user_queries": {
                "get_users": {
                    "query": """
query GetUsers($limit: Int, $offset: Int, $search: String) {
  users(limit: $limit, offset: $offset, search: $search) {
    id
    username
    displayName
    firstName
    lastName
    email
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
                    """,
                    "variables": {
                        "limit": 10,
                        "offset": 0,
                        "search": "john"
                    },
                    "description": "Get users with pagination and search"
                },
                "get_user_edit_form": {
                    "query": """
query GetUserEditForm($userId: Int!) {
  userEditForm(userId: $userId) {
    id
    username
    email
    displayName
    firstName
    lastName
    isActive
    roleId
    clientId
    availableRoles {
      id
      roleName
      roleCode
    }
    availableClients {
      id
      name
      code
    }
  }
}
                    """,
                    "variables": {
                        "userId": 1
                    },
                    "description": "Get user data for edit form with available options"
                }
            },
            "user_mutations": {
                "create_users_bulk": {
                    "mutation": """
mutation CreateUsersBulk($input: BulkUserCreateInput!) {
  createUsersBulk(input: $input) {
    success
    message
    totalProcessed
    successCount
    failureCount
    createdUsers {
      id
      username
      displayName
      firstName
      lastName
      email
    }
    failedUsers
  }
}
                    """,
                    "variables": {
                        "input": {
                            "forms": {
                                "form_1": {
                                    "first_name": "John",
                                    "last_name": "Doe",
                                    "email": "john.doe@example.com",
                                    "job_title": "Software Engineer",
                                    "role_id": 1
                                },
                                "form_2": {
                                    "first_name": "Jane",
                                    "last_name": "Smith",
                                    "email": "jane.smith@example.com",
                                    "job_title": "Product Manager",
                                    "role_id": 2
                                }
                            }
                        }
                    },
                    "description": "Create multiple users at once"
                }
            },
            "role_queries": {
                "get_roles": {
                    "query": """
query GetRoles($search: String, $statusFilter: String, $limit: Int) {
  roles(search: $search, statusFilter: $statusFilter, limit: $limit) {
    id
    roleName
    roleCode
    description
    isActive
    createdAt
  }
}
                    """,
                    "variables": {
                        "search": "admin",
                        "statusFilter": "active",
                        "limit": 10
                    },
                    "description": "Get roles with search and filtering"
                },
                "get_role_details": {
                    "query": """
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
      username
      displayName
      email
    }
    modulePermissions
  }
}
                    """,
                    "variables": {
                        "roleId": 1
                    },
                    "description": "Get detailed role information with users and permissions"
                },
                "get_roles_summary": {
                    "query": """
query GetRolesSummary($page: Int, $pageSize: Int) {
  rolesSummary(page: $page, pageSize: $pageSize) {
    roles {
      roleId
      roleName
      roleCode
      totalUsers
      products
      status
      isActive
    }
    totalCount
    page
    pageSize
    totalPages
  }
}
                    """,
                    "variables": {
                        "page": 1,
                        "pageSize": 10
                    },
                    "description": "Get aggregated role data with pagination"
                }
            },
            "role_mutations": {
                "create_role": {
                    "mutation": """
mutation CreateRole($input: RoleCreateInput!) {
  createRole(input: $input) {
    success
    message
    role {
      id
      roleName
      roleCode
      description
      isActive
    }
    errors
  }
}
                    """,
                    "variables": {
                        "input": {
                            "roleName": "Auditor",
                            "roleCode": "auditor",
                            "description": "Auditor role with limited permissions",
                            "isActive": True,
                            "permissions": [
                                {
                                    "module": "Frame",
                                    "create": True,
                                    "view": True,
                                    "update": False,
                                    "delete": False
                                },
                                {
                                    "module": "NAV Validus",
                                    "create": False,
                                    "view": True,
                                    "update": False,
                                    "delete": False
                                }
                            ]
                        }
                    },
                    "description": "Create a new role with permissions"
                }
            }
        }
    
    def generate_markdown_documentation(self) -> str:
        """Generate markdown documentation"""
        operations = self.generate_operations_documentation()
        
        doc = f"""# GraphQL API Documentation

Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Overview

This document provides comprehensive documentation for the GraphQL API endpoints. The API supports both user management and role management operations.

**GraphQL Endpoint**: `{self.graphql_url}`

## Quick Start

### 1. GraphQL Playground
Visit `{self.graphql_url}` in your browser for an interactive GraphQL playground with:
- Schema explorer
- Query builder
- Auto-completion
- Documentation

### 2. Authentication
All operations require authentication. Include authentication headers in your requests.

## Operations

### Queries

"""
        
        # Add queries documentation
        for query in operations["queries"]:
            doc += f"""
#### {query['name']}

**Description**: {query.get('description', 'No description available')}

**Arguments**:
"""
            for arg in query.get('args', []):
                doc += f"- `{arg['name']}` ({arg['type']}): {arg.get('description', 'No description')}\n"
            
            doc += f"\n**Return Type**: {query['type']}\n\n"
        
        # Add mutations documentation
        doc += "\n### Mutations\n\n"
        for mutation in operations["mutations"]:
            doc += f"""
#### {mutation['name']}

**Description**: {mutation.get('description', 'No description available')}

**Arguments**:
"""
            for arg in mutation.get('args', []):
                doc += f"- `{arg['name']}` ({arg['type']}): {arg.get('description', 'No description')}\n"
            
            doc += f"\n**Return Type**: {mutation['type']}\n\n"
        
        # Add examples
        doc += "\n## Examples\n\n"
        for category, examples in operations["examples"].items():
            doc += f"### {category.replace('_', ' ').title()}\n\n"
            for example_name, example_data in examples.items():
                doc += f"#### {example_name.replace('_', ' ').title()}\n\n"
                doc += f"**Description**: {example_data['description']}\n\n"
                query_or_mutation = example_data.get('query') or example_data.get('mutation', '')
                doc += f"**Query/Mutation**:\n```graphql\n{query_or_mutation}\n```\n\n"
                doc += f"**Variables**:\n```json\n{json.dumps(example_data['variables'], indent=2)}\n```\n\n"
        
        # Add types documentation
        doc += "\n## Types\n\n"
        for type_name, type_info in operations["types"].items():
            if type_name in ["Query", "Mutation"]:
                continue
            doc += f"### {type_name}\n\n"
            doc += f"**Kind**: {type_info['kind']}\n\n"
            if type_info.get('description'):
                doc += f"**Description**: {type_info['description']}\n\n"
            
            if type_info.get('fields'):
                doc += "**Fields**:\n\n"
                for field in type_info['fields']:
                    doc += f"- `{field['name']}` ({field['type']}): {field.get('description', 'No description')}\n"
                doc += "\n"
        
        doc += """
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

## Best Practices

1. **Use Variables**: Always use variables for dynamic values
2. **Request Only Needed Fields**: GraphQL allows you to request only the fields you need
3. **Handle Errors**: Always check for errors in the response
4. **Use Pagination**: For large datasets, use pagination parameters
5. **Cache Results**: Implement appropriate caching strategies

## Frontend Integration

### Apollo Client (React)
```javascript
import { ApolloClient, InMemoryCache, gql } from '@apollo/client';

const client = new ApolloClient({
  uri: 'http://localhost:8000/graphql',
  cache: new InMemoryCache()
});

const GET_USERS = gql`
  query GetUsers($limit: Int, $offset: Int) {
    users(limit: $limit, offset: $offset) {
      id
      username
      displayName
    }
  }
`;
```

### Relay (React)
```javascript
import { graphql } from 'react-relay';

const GetUsersQuery = graphql`
  query GetUsersQuery($limit: Int, $offset: Int) {
    users(limit: $limit, offset: $offset) {
      id
      username
      displayName
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

fetch('http://localhost:8000/graphql', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    query,
    variables: { limit: 10, offset: 0 }
  })
});
```

## Support

For questions or issues, please contact the backend team or refer to the GraphQL playground for interactive exploration.
"""
        
        return doc
    
    def save_documentation(self, filename: str = "graphql_api_documentation.md"):
        """Save documentation to file"""
        doc = self.generate_markdown_documentation()
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(doc)
        
        print(f"Documentation saved to: {filename}")
    
    def generate_typescript_types(self) -> str:
        """Generate TypeScript type definitions"""
        operations = self.generate_operations_documentation()
        
        typescript = """// Generated TypeScript types for GraphQL API
// Generated on: """ + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + """

export interface User {
  id: number;
  username: string;
  displayName: string;
  firstName: string;
  lastName: string;
  email?: string;
  isActive: boolean;
  createdAt: string;
  updatedAt: string;
  role?: Role;
  client?: Client;
}

export interface Role {
  id: number;
  roleName: string;
  roleCode: string;
  description?: string;
  isActive: boolean;
  createdAt: string;
  updatedAt: string;
}

export interface Client {
  id: number;
  name: string;
  code: string;
  description?: string;
  type?: string;
  isActive: boolean;
  createdAt: string;
  updatedAt: string;
}

export interface RoleDetail {
  id: number;
  roleName: string;
  roleCode: string;
  description?: string;
  isActive: boolean;
  createdAt: string;
  updatedAt: string;
  users: User[];
  permissions: RoleModulePermission[];
  userCount: number;
  modulePermissions: Record<string, Record<string, boolean>>;
}

export interface RoleSummary {
  roleId: number;
  roleName: string;
  roleCode: string;
  totalUsers: number;
  products: string;
  status: string;
  isActive: boolean;
  createdAt: string;
}

export interface RoleSummaryResponse {
  roles: RoleSummary[];
  totalCount: number;
  page: number;
  pageSize: number;
  totalPages: number;
}

// Input Types
export interface BulkUserCreateInput {
  forms: Record<string, {
    first_name: string;
    last_name: string;
    email: string;
    job_title: string;
    role_id: number;
  }>;
}

export interface RoleCreateInput {
  roleName: string;
  roleCode: string;
  description?: string;
  isActive?: boolean;
  permissions?: Array<{
    module: string;
    create?: boolean;
    view?: boolean;
    update?: boolean;
    delete?: boolean;
  }>;
}

// Response Types
export interface BulkUserCreateResult {
  success: boolean;
  message: string;
  totalProcessed: number;
  successCount: number;
  failureCount: number;
  createdUsers: User[];
  failedUsers: Array<{
    form_key: string;
    data: any;
    error: string;
  }>;
}

export interface RoleCreateResult {
  success: boolean;
  message: string;
  role?: Role;
  errors?: string[];
}

// Query/Mutation Variables
export interface GetUsersVariables {
  id?: number;
  search?: string;
  statusFilter?: string;
  limit?: number;
  offset?: number;
}

export interface GetRoleDetailsVariables {
  roleId: number;
}

export interface GetRolesSummaryVariables {
  page?: number;
  pageSize?: number;
}

export interface CreateUsersBulkVariables {
  input: BulkUserCreateInput;
}

export interface CreateRoleVariables {
  input: RoleCreateInput;
}
"""
        
        return typescript

if __name__ == "__main__":
    print("=== GraphQL Documentation Generator ===\n")
    
    # Initialize generator
    doc_gen = GraphQLDocumentationGenerator()
    
    # Generate documentation
    print("1. Introspecting GraphQL schema...")
    schema = doc_gen.introspect_schema()
    
    if schema:
        print("✓ Schema introspection successful")
        
        print("\n2. Generating operations documentation...")
        operations = doc_gen.generate_operations_documentation()
        print(f"✓ Found {len(operations['queries'])} queries and {len(operations['mutations'])} mutations")
        
        print("\n3. Saving documentation...")
        doc_gen.save_documentation("GRAPHQL_API_DOCUMENTATION.md")
        
        print("\n4. Generating TypeScript types...")
        typescript_types = doc_gen.generate_typescript_types()
        with open("graphql_types.ts", 'w', encoding='utf-8') as f:
            f.write(typescript_types)
        print("✓ TypeScript types saved to: graphql_types.ts")
        
        print("\n=== Documentation Generation Complete ===")
        print("\nFiles generated:")
        print("- GRAPHQL_API_DOCUMENTATION.md (Complete API documentation)")
        print("- graphql_types.ts (TypeScript type definitions)")
        print("\nInteractive documentation available at: http://localhost:8000/graphql")
        
    else:
        print("✗ Failed to introspect schema. Make sure the GraphQL server is running.")
