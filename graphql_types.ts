// Generated TypeScript types for GraphQL API
// Last updated: 2025-10-10

export interface RoleModulePermission {
  id: number;
  roleId?: number | null;
  clientId?: number | null;
  moduleId: number;
  masterId?: number | null;
  permissionId: number;
  isActive: boolean;
  module: {
    id: number;
    moduleName: string;
    moduleCode: string;
    description?: string;
  };
  permission: {
    id: number;
    permissionName: string;
    permissionCode: string;
    description?: string;
  };
}

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

// Document Types
export interface Document {
  id: number;
  name: string;
  type?: string;
  path: string;
  size?: number;
  status: string;
  fundId?: number;
  uploadDate: string;
  replay: boolean;
  createdBy?: string;
  metadata?: any;
  isActive: boolean;
  createdAt: string;
  updatedAt: string;
}

export interface DocumentCreateInput {
  name: string;
  type?: string;
  path: string;
  size?: number;
  status?: string;
  fundId?: number;
  replay?: boolean;
  createdBy?: string;
  metadata?: any;
}

export interface DocumentUpdateInput {
  name?: string;
  type?: string;
  path?: string;
  size?: number;
  status?: string;
  fundId?: number;
  replay?: boolean;
  createdBy?: string;
  metadata?: any;
  isActive?: boolean;
}

export interface DocumentFilterInput {
  name?: string;
  type?: string;
  status?: string;
  fundId?: number;
  replay?: boolean;
  createdBy?: string;
  isActive?: boolean;
}

export interface DocumentResponse {
  success: boolean;
  message: string;
  document?: Document;
}

export interface DocumentListResponse {
  success: boolean;
  message: string;
  documents: Document[];
  total: number;
}

// Document Query/Mutation Variables
export interface GetDocumentsVariables {
  filter?: DocumentFilterInput;
  limit?: number;
  offset?: number;
}

export interface GetDocumentVariables {
  documentId: number;
}

export interface GetDocumentsByFundVariables {
  fundId: number;
  limit?: number;
  offset?: number;
}

export interface CreateDocumentVariables {
  input: DocumentCreateInput;
}

export interface UpdateDocumentVariables {
  documentId: number;
  input: DocumentUpdateInput;
}

export interface DeleteDocumentVariables {
  documentId: number;
}
