# Validation and Ratio API - Postman Test Requests

## ⚠️ Important: Field Name Convention

**GraphQL uses camelCase for ALL field names:**
- In queries/responses: Use `vcsourcetype` instead of `bitsourceSingle`/`bitsourceDual`
- In input variables: Use `vcsourcetype` with value "Single" or "Dual"

**Important Schema Changes:**
- `bitsourceSingle` and `bitsourceDual` → replaced by `vcsourcetype` (single field with value "Single" or "Dual")
- `vcformula`, `vcfilter`, `isfilter`, `vcfiltertype` → moved to ValidationDetails/RatioDetails tables
- New fields: `issubtypeSubtotal`, `isvalidationSubtotal`, `isratioSubtotal` (GraphQL uses camelCase)
- Boolean fields (e.g., `isactive`, `isvalidationSubtotal`, `ismandatory`) are now native boolean types (true/false) instead of Bit fields
- Database field names use snake_case; GraphQL converts to camelCase automatically
- **New Threshold Range Fields (added 2025-01-15):**
  - `vcthreshold_abs_range`: String(20) - Threshold absolute range type
  - `intthresholdmin`: Decimal(30,6) - Minimum threshold value
  - `intthresholdmax`: Decimal(30,6) - Maximum threshold value
  - Available in both `ValidationMaster` and `RatioMaster` tables

**Strawberry automatically converts Python snake_case to GraphQL camelCase everywhere in the schema.**

---

## Setup Instructions

1. **Method**: POST
2. **URL**: `http://localhost:8000/graphql` (or your server URL)
3. **Headers**:
   ```
   Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJkZW1vIiwiZXhwIjoxNzYxNjY1NDk0fQ.qdAhoYiv7qjtDPU6bKNf4SVmrDWYYh5NckXg7XHFav8
   Content-Type: application/json
   ```

---

## Query Tests

### 1. Get All Subproducts

```json
{
  "query": "query GetSubproducts { getSubproducts { intsubproductid vcsubproductname vcdescription isactive intcreatedby dtcreatedat intupdatedby dtupdatedat } }"
}
```

**Response:**
```json
{
  "data": {
    "getSubproducts": [
      {
        "intsubproductid": 1,
        "vcsubproductname": "NAV",
        "vcdescription": "Net Asset Value validation",
        "isactive": true,
        "intcreatedby": 1,
        "dtcreatedat": "2025-01-15T10:30:00",
        "intupdatedby": 1,
        "dtupdatedat": "2025-01-20T14:45:00"
      },
      {
        "intsubproductid": 2,
        "vcsubproductname": "Private Credit",
        "vcdescription": "Trial balance validations",
        "isactive": true,
        "intcreatedby": 1,
        "dtcreatedat": "2025-01-16T09:00:00",
        "intupdatedby": null,
        "dtupdatedat": null
      }
    ]
  }
}
```

---

### 2. Get Validation Details for Subproduct ID 1

```json
{
  "query": "query GetValidationDetails($intsubproductid: Int) { getSubproductDetailsForValidation(intsubproductid: $intsubproductid) { intsubproductdetailid intsubproductid vcvalidustype vctype vcsubtype vcdescription isactive intcreatedby dtcreatedat intupdatedby dtupdatedat } }",
  "variables": {
    "intsubproductid": 1
  }
}
```

---

### 3. Get All Validation Details (No Filter)

```json
{
  "query": "query GetAllValidationDetails { getSubproductDetailsForValidation { intsubproductdetailid intsubproductid vcvalidustype vctype vcsubtype isactive } }"
}
```

---

### 4. Get Ratio Details for Subproduct ID 1

```json
{
  "query": "query GetRatioDetails($intsubproductid: Int) { getSubproductDetailsForRatio(intsubproductid: $intsubproductid) { intsubproductdetailid intsubproductid vcvalidustype vctype vcsubtype vcdescription isactive intcreatedby dtcreatedat intupdatedby dtupdatedat } }",
  "variables": {
    "intsubproductid": 1
  }
}
```

---

### 5. Get All Ratio Details (No Filter)

```json
{
  "query": "query GetAllRatioDetails { getSubproductDetailsForRatio { intsubproductdetailid intsubproductid vcvalidustype vctype vcsubtype isactive } }"
}
```

---

### 6. Get All Validations with Pagination

**Note:** Use camelCase for field names in GraphQL queries. The `getAllValidations` query now returns a paginated response with `validations` list and `pagination` information.

#### 6a. Get All Validations (Default Pagination)

Uses default page 1 with 10 items per page:

```json
{
  "query": "query GetAllValidations { getAllValidations { validations { intvalidationmasterid intsubproductid vcsourcetype vctype vcsubtype issubtypeSubtotal vcvalidationname isvalidationSubtotal vcdescription intthreshold vcthresholdtype vcthresholdAbsRange intthresholdmin intthresholdmax intprecision isactive intcreatedby dtcreatedat intupdatedby dtupdatedat } pagination { pageNumber pageSize currentPage totalPages totalCount } } }"
}
```

**Response:**
```json
{
  "data": {
    "getAllValidations": {
      "validations": [
        {
          "intvalidationmasterid": 1,
          "intsubproductid": 1,
          "vcsourcetype": "Single",
          "vctype": "P&L",
          "vcsubtype": "Market Value",
          "issubtypeSubtotal": false,
          "vcvalidationname": "Market Value Validation",
          "isvalidationSubtotal": false,
          "vcdescription": "Validates market value calculations",
          "intthreshold": 0.05,
          "vcthresholdtype": "Percentage",
          "intprecision": 4.0,
          "isactive": true,
          "intcreatedby": 1,
          "dtcreatedat": "2025-01-15T10:30:00",
          "intupdatedby": 1,
          "dtupdatedat": "2025-01-20T14:45:00"
        },
        {
          "intvalidationmasterid": 2,
          "intsubproductid": 1,
          "vcsourcetype": "Dual",
          "vctype": "P&L",
          "vcsubtype": "Pricing",
          "issubtypeSubtotal": true,
          "vcvalidationname": "Price Comparison Validation",
          "isvalidationSubtotal": true,
          "vcdescription": "Compares prices from multiple sources",
          "intthreshold": 0.01,
          "vcthresholdtype": "Percentage",
          "intprecision": 4.0,
          "isactive": true,
          "intcreatedby": 1,
          "dtcreatedat": "2025-01-16T09:00:00",
          "intupdatedby": null,
          "dtupdatedat": null
        }
      ],
      "pagination": {
        "pageNumber": 1,
        "pageSize": 10,
        "currentPage": 1,
        "totalPages": 3,
        "totalCount": 25
      }
    }
  }
}
```

#### 6b. Get Validations with Custom Pagination

Specify page number and page size:

```json
{
  "query": "query GetAllValidations($pageNumber: Int, $pageSize: Int) { getAllValidations(pageNumber: $pageNumber, pageSize: $pageSize) { validations { intvalidationmasterid vcvalidationname vctype vcsubtype vcsourcetype isactive } pagination { pageNumber pageSize currentPage totalPages totalCount } } }",
  "variables": {
    "pageNumber": 2,
    "pageSize": 5
  }
}
```

**Response:**
```json
{
  "data": {
    "getAllValidations": {
      "validations": [
        {
          "intvalidationmasterid": 6,
          "vcvalidationname": "Validation 6",
          "vctype": "P&L",
          "vcsubtype": "Revenue",
          "vcsourcetype": "Single",
          "isactive": true
        }
      ],
      "pagination": {
        "pageNumber": 2,
        "pageSize": 5,
        "currentPage": 2,
        "totalPages": 5,
        "totalCount": 25
      }
    }
  }
}
```

#### 6c. Get Validations by Subproduct with Pagination

Filter by subproduct ID and paginate:

```json
{
  "query": "query GetValidationsBySubproduct($intsubproductid: Int, $pageNumber: Int, $pageSize: Int) { getAllValidations(intsubproductid: $intsubproductid, pageNumber: $pageNumber, pageSize: $pageSize) { validations { intvalidationmasterid vcvalidationname vctype vcsubtype vcsourcetype issubtypeSubtotal isvalidationSubtotal intthreshold vcthresholdtype vcthresholdAbsRange intthresholdmin intthresholdmax intprecision isactive } pagination { pageNumber pageSize currentPage totalPages totalCount } } }",
  "variables": {
    "intsubproductid": 1,
    "pageNumber": 1,
    "pageSize": 20
  }
}
```

**Response:**
```json
{
  "data": {
    "getAllValidations": {
      "validations": [
        {
          "intvalidationmasterid": 1,
          "vcvalidationname": "Market Value Validation",
          "vctype": "P&L",
          "vcsubtype": "Market Value",
          "vcsourcetype": "Single",
          "issubtypeSubtotal": false,
          "isvalidationSubtotal": false,
          "intthreshold": 0.05,
          "vcthresholdtype": "Percentage",
          "intprecision": 4.0,
          "isactive": true
        }
      ],
      "pagination": {
        "pageNumber": 1,
        "pageSize": 20,
        "currentPage": 1,
        "totalPages": 1,
        "totalCount": 15
      }
    }
  }
}
```

---

### 7. Get Validations by Subproduct

**Note:** This query now returns paginated results. See section 6c above for an example with pagination. Below is a simple example without pagination parameters (uses defaults):

```json
{
    "query": "query GetValidationsBySubproduct($intsubproductid: Int) { getAllValidations(intsubproductid: $intsubproductid) { validations { intvalidationmasterid vcvalidationname vctype vcsubtype vcsourcetype issubtypeSubtotal isvalidationSubtotal intthreshold vcthresholdtype intprecision isactive } pagination { pageNumber pageSize currentPage totalPages totalCount } } }",
  "variables": {
    "intsubproductid": 1
  }
}
```

---

### 8. Get All Ratios with Pagination

**Note:** Use camelCase for field names in GraphQL queries. The `getAllRatios` query returns a paginated response with `ratios` list and `pagination` information.

#### 8a. Get All Ratios (Default Pagination)

Uses default page 1 with 10 items per page:

```json
{
  "query": "query GetAllRatios { getAllRatios { ratios { intratiomasterid intsubproductid vcsourcetype vctype vcrationame isratioSubtotal vcdescription intthreshold vcthresholdtype vcthresholdAbsRange intthresholdmin intthresholdmax intprecision isactive intcreatedby dtcreatedat intupdatedby dtupdatedat } pagination { pageNumber pageSize currentPage totalPages totalCount } } }"
}
```

**Response:**
```json
{
  "data": {
    "getAllRatios": {
      "ratios": [
        {
          "intratiomasterid": 1,
          "intsubproductid": 1,
          "vcsourcetype": "Single",
          "vctype": "Financial",
          "vcrationame": "Debt to Equity Ratio",
          "isratioSubtotal": false,
          "vcdescription": "Calculates debt to equity ratio",
          "intthreshold": 2.0,
          "vcthresholdtype": "Absolute",
          "intprecision": 4.0,
          "isactive": true,
          "intcreatedby": 1,
          "dtcreatedat": "2025-01-15T10:30:00",
          "intupdatedby": 1,
          "dtupdatedat": "2025-01-20T14:45:00"
        },
        {
          "intratiomasterid": 2,
          "intsubproductid": 1,
          "vcsourcetype": "Single",
          "vctype": "Liquidity",
          "vcrationame": "Current Ratio",
          "isratioSubtotal": false,
          "vcdescription": "Measures ability to pay short-term obligations",
          "intthreshold": 1.5,
          "vcthresholdtype": "Absolute",
          "intprecision": 4.0,
          "isactive": true,
          "intcreatedby": 1,
          "dtcreatedat": "2025-01-16T09:00:00",
          "intupdatedby": null,
          "dtupdatedat": null
        }
      ],
      "pagination": {
        "pageNumber": 1,
        "pageSize": 10,
        "currentPage": 1,
        "totalPages": 2,
        "totalCount": 18
      }
    }
  }
}
```

#### 8b. Get Ratios with Custom Pagination

Specify page number and page size:

```json
{
  "query": "query GetAllRatios($pageNumber: Int, $pageSize: Int) { getAllRatios(pageNumber: $pageNumber, pageSize: $pageSize) { ratios { intratiomasterid vcrationame vctype vcsourcetype isactive } pagination { pageNumber pageSize currentPage totalPages totalCount } } }",
  "variables": {
    "pageNumber": 1,
    "pageSize": 5
  }
}
```

**Response:**
```json
{
  "data": {
    "getAllRatios": {
      "ratios": [
        {
          "intratiomasterid": 6,
          "vcrationame": "Ratio 6",
          "vctype": "Financial",
          "vcsourcetype": "Single",
          "isactive": true
        }
      ],
      "pagination": {
        "pageNumber": 2,
        "pageSize": 5,
        "currentPage": 2,
        "totalPages": 4,
        "totalCount": 18
      }
    }
  }
}
```

#### 8c. Get Ratios by Subproduct with Pagination

Filter by subproduct ID and paginate:

```json
{
  "query": "query GetRatiosBySubproduct($intsubproductid: Int, $pageNumber: Int, $pageSize: Int) { getAllRatios(intsubproductid: $intsubproductid, pageNumber: $pageNumber, pageSize: $pageSize) { ratios { intratiomasterid vcrationame vctype vcsourcetype isratioSubtotal intthreshold vcthresholdtype intprecision isactive } pagination { pageNumber pageSize currentPage totalPages totalCount } } }",
  "variables": {
    "intsubproductid": 1,
    "pageNumber": 1,
    "pageSize": 20
  }
}
```

**Response:**
```json
{
  "data": {
    "getAllRatios": {
      "ratios": [
        {
          "intratiomasterid": 1,
          "vcrationame": "Debt to Equity Ratio",
          "vctype": "Financial",
          "vcsourcetype": "Single",
          "isratioSubtotal": false,
          "intthreshold": 2.0,
          "vcthresholdtype": "Absolute",
          "intprecision": 4.0,
          "isactive": true
        }
      ],
      "pagination": {
        "pageNumber": 1,
        "pageSize": 20,
        "currentPage": 1,
        "totalPages": 1,
        "totalCount": 12
      }
    }
  }
}
```
#### 8a. Get All Ratios with Pagination
```json
{
  "query": "query GetAllRatios($pageNumber: Int, $pageSize: Int) { getAllRatios(pageNumber: $pageNumber, pageSize: $pageSize) { ratios { intratiomasterid intsubproductid vcsourcetype vctype vcrationame isratioSubtotal vcdescription intthreshold vcthresholdtype intprecision isactive intcreatedby dtcreatedat intupdatedby dtupdatedat } pagination { pageNumber pageSize currentPage totalPages totalCount } } }",
  "variables": {
    "intsubproductid": 1,
    "pageNumber": 1,
    "pageSize": 2
  }
}
```

Response:
```json
{
    "data": {
        "getAllRatios": {
            "ratios": [
                {
                    "intratiomasterid": 1,
                    "intsubproductid": 1,
                    "vcsourcetype": "Dual",
                    "vctype": "Financial",
                    "vcrationame": "Updated: Debt to Equity Ratio",
                    "isratioSubtotal": true,
                    "vcdescription": "Updated financial ratio description",
                    "intthreshold": 2.5,
                    "vcthresholdtype": "Absolute",
                    "intprecision": 4.0,
                    "isactive": true,
                    "intcreatedby": null,
                    "dtcreatedat": "2025-10-28T13:40:46.334994",
                    "intupdatedby": null,
                    "dtupdatedat": "2025-10-28T19:01:32.201862"
                },
                {
                    "intratiomasterid": 2,
                    "intsubproductid": 1,
                    "vcsourcetype": "Single",
                    "vctype": "Liquidity",
                    "vcrationame": "Updated: Current Ratio",
                    "isratioSubtotal": false,
                    "vcdescription": "Measures ability to pay short-term obligations",
                    "intthreshold": 1.5,
                    "vcthresholdtype": "Absolute",
                    "intprecision": 4.0,
                    "isactive": true,
                    "intcreatedby": null,
                    "dtcreatedat": "2025-10-28T13:40:59.496143",
                    "intupdatedby": null,
                    "dtupdatedat": "2025-10-28T19:04:04.034470"
                }
            ],
            "pagination": {
                "pageNumber": 1,
                "pageSize": 2,
                "currentPage": 1,
                "totalPages": 1,
                "totalCount": 2
            }
        }
    }
}
```
---

### 9. Get Ratios by Subproduct

**Note:** This query now returns paginated results. See section 8c above for an example with pagination. Below is a simple example without pagination parameters (uses defaults):

```json
{
  "query": "query GetRatiosBySubproduct($intsubproductid: Int) { getAllRatios(intsubproductid: $intsubproductid) { ratios { intratiomasterid vcrationame vctype vcsourcetype isratioSubtotal intthreshold vcthresholdtype intprecision isactive } pagination { pageNumber pageSize currentPage totalPages totalCount } } }",
  "variables": {
    "intsubproductid": 1
  }
}
```

---

### 9a. Get Validation by ID with Details

Get a validation with all its details for editing:

```json
{
  "query": "query GetValidationById($intvalidationmasterid: Int!) { getValidationById(intvalidationmasterid: $intvalidationmasterid) { intvalidationmasterid intsubproductid vcsourcetype vctype vcsubtype issubtypeSubtotal vcvalidationname isvalidationSubtotal vcdescription intthreshold vcthresholdtype vcthresholdAbsRange intthresholdmin intthresholdmax intprecision isactive intcreatedby dtcreatedat intupdatedby dtupdatedat details { intvalidationdetailid intdatamodelid intgroupAttributeid intassettypeid intcalcAttributeid vcaggregationtype vcfilter vcfiltertype vcformula } } }",
  "variables": {
    "intvalidationmasterid": 1
  }
}
```

**Response:**
```json
{
  "data": {
    "getValidationById": {
      "intvalidationmasterid": 1,
      "intsubproductid": 1,
      "vcsourcetype": "Single",
      "vctype": "P&L",
      "vcsubtype": "Market Value",
      "issubtypeSubtotal": false,
      "vcvalidationname": "Market Value Validation",
      "isvalidationSubtotal": false,
      "vcdescription": "Validates market value calculations",
      "intthreshold": 0.05,
      "vcthresholdtype": "Percentage",
      "vcthresholdAbsRange": null,
      "intthresholdmin": null,
      "intthresholdmax": null,
      "intprecision": 4.0,
      "isactive": true,
      "intcreatedby": 1,
      "dtcreatedat": "2025-01-15T10:30:00",
      "intupdatedby": 1,
      "dtupdatedat": "2025-01-20T14:45:00",
      "details": [
        {
          "intvalidationdetailid": 5,
          "intdatamodelid": 1,
          "intgroupAttributeid": 10,
          "intassettypeid": 15,
          "intcalcAttributeid": 20,
          "vcaggregationtype": "sum",
          "vcfilter": "asset_class = 'Equity'",
          "vcfiltertype": "I",
I          "vcformula": "SUM(market_value)"
        },
        {
          "intvalidationdetailid": 6,
          "intdatamodelid": 1,
          "intgroupAttributeid": 11,
          "intassettypeid": 16,
          "intcalcAttributeid": 21,
          "vcaggregationtype": "avg",
          "vcfilter": null,
          "vcfiltertype": null,
          "vcformula": "AVG(price)"
        }
      ]
    }
  }
}
```

---

### 9b. Get Ratio by ID with Details

Get a ratio with all its details for editing:

```json
{
  "query": "query GetRatioById($intratiomasterid: Int!) { getRatioById(intratiomasterid: $intratiomasterid) { intratiomasterid intsubproductid vcsourcetype vctype vcrationame isratioSubtotal vcdescription intthreshold vcthresholdtype vcthresholdAbsRange intthresholdmin intthresholdmax intprecision isactive intcreatedby dtcreatedat intupdatedby dtupdatedat details { intratiodetailid intdatamodelid vcfilter vcfiltertype vcnumerator vcdenominator vcformula } } }",
  "variables": {
    "intratiomasterid": 1
  }
}
```

**Response:**
```json
{
  "data": {
    "getRatioById": {
      "intratiomasterid": 1,
      "intsubproductid": 1,
      "vcsourcetype": "Single",
      "vctype": "Financial",
      "vcrationame": "Debt to Equity Ratio",
      "isratioSubtotal": false,
      "vcdescription": "Calculates debt to equity ratio",
      "intthreshold": 2.0,
      "vcthresholdtype": "Absolute",
      "intprecision": 4.0,
      "isactive": true,
      "intcreatedby": 1,
      "dtcreatedat": "2025-01-15T10:30:00",
      "intupdatedby": 1,
      "dtupdatedat": "2025-01-20T14:45:00",
      "details": [
        {
          "intratiodetailid": 3,
          "intdatamodelid": 2,
          "vcfilter": "asset_type = 'Investment'",
          "vcfiltertype": "I",
          "vcnumerator": "Displayname: Total Debt\n(SUM(CASE WHEN {'Trial Balance'}.[Account Type] = 'Liabilities' THEN ABS({'Trial Balance'}.[Closing Balance]) ELSE 0 END))",
          "vcdenominator": "Displayname: Total Equity\n(SUM(CASE WHEN {'Trial Balance'}.[Account Type] = 'Equity' THEN {'Trial Balance'}.[Closing Balance] ELSE 0 END))",
          "vcformula": "((Period2.`Numerator`/Period2.`Denominator` - Period1.`Numerator`/Period1.`Denominator`)*100)/(Period1.`Numerator`/Period1.`Denominator`)"
        }
      ]
    }
  }
}
```

---

### 9c. Get Data Load Dates

Get list of dates (dtdataasof) from tbl_dataload_instance table filtered by client_id, fund_id, and source(s).

**Single Source:**
```json
{
  "query": "query GetDataLoadDates($clientId: Int!, $fundId: Int!, $source: String) { getDataLoadDates(clientId: $clientId, fundId: $fundId, source: $source) { date } }",
  "variables": {
    "clientId": 2,
    "fundId": 1,
    "source": "SourceA"
  }
}
```

**Response (Single Source):**
```json
{
  "data": {
    "getDataLoadDates": [
      {
        "date": "2024-01-15"
      },
      {
        "date": "2024-02-15"
      },
      {
        "date": "2024-03-15"
      }
    ]
  }
}
```

**Two Sources (Intersection - dates present in BOTH sources):**
```json
{
  "query": "query GetDataLoadDates($clientId: Int!, $fundId: Int!, $source: String, $source2: String) { getDataLoadDates(clientId: $clientId, fundId: $fundId, source: $source, source2: $source2) { date } }",
  "variables": {
    "clientId": 2,
    "fundId": 1,
    "source": "SourceA",
    "source2": "SourceB"
  }
}
```

**Response (Two Sources - Intersection):**
```json
{
  "data": {
    "getDataLoadDates": [
      {
        "date": "2024-01-15"
      },
      {
        "date": "2024-03-15"
      }
    ]
  }
}
```

> **Note:** When two sources are provided, only dates that exist in BOTH sources are returned (intersection). If a date exists in SourceA but not in SourceB, it will not be included in the results.

---

## Fund Management Query Tests

### 1. Get All Funds

Get a list of all funds from the `public.funds` table with pagination and filtering options.

```json
{
  "query": "query GetFunds($limit: Int, $offset: Int, $search: String, $statusFilter: String) { funds(limit: $limit, offset: $offset, search: $search, statusFilter: $statusFilter) { id name code description type fundManager baseCurrency contactPerson contactEmail contactNumber sector geography strategy marketCap stage inceptionDate investmentStartDate commitmentSubscription isActive createdAt updatedAt } }",
  "variables": {
    "limit": 10,
    "offset": 0,
    "search": null,
    "statusFilter": "active"
  }
}
```

**Response:**
```json
{
  "data": {
    "funds": [
      {
        "id": 1,
        "name": "Global Equity Fund",
        "code": "GEF001",
        "description": "A diversified global equity fund",
        "type": "Equity",
        "fundManager": "ABC Asset Management",
        "baseCurrency": "USD",
        "contactPerson": "John Doe",
        "contactEmail": "john.doe@example.com",
        "contactNumber": "+1-555-0123",
        "sector": "Technology",
        "geography": "Global",
        "strategy": {
          "strategies": ["Long/Short", "Market Neutral"]
        },
        "marketCap": "Large Cap",
        "stage": "Active",
        "inceptionDate": "2020-01-15",
        "investmentStartDate": "2020-02-01",
        "commitmentSubscription": 1000000.0,
        "isActive": true,
        "createdAt": "2020-01-10T10:00:00",
        "updatedAt": "2024-12-01T14:30:00"
      },
      {
        "id": 2,
        "name": "Fixed Income Fund",
        "code": "FIF002",
        "description": "Corporate bond focused fund",
        "type": "Fixed Income",
        "fundManager": "XYZ Capital",
        "baseCurrency": "USD",
        "contactPerson": "Jane Smith",
        "contactEmail": "jane.smith@example.com",
        "contactNumber": "+1-555-0456",
        "sector": "Corporate Bonds",
        "geography": "North America",
        "strategy": null,
        "marketCap": null,
        "stage": "Active",
        "inceptionDate": "2019-06-01",
        "investmentStartDate": "2019-07-01",
        "commitmentSubscription": 5000000.0,
        "isActive": true,
        "createdAt": "2019-05-20T09:00:00",
        "updatedAt": "2024-11-15T16:20:00"
      }
    ]
  }
}
```

---

### 2. Get Funds by Client ID

Get all funds associated with a specific client from the `client_funds` association table.

```json
{
  "query": "query GetFundsByClient($clientId: Int!, $limit: Int, $offset: Int, $search: String, $statusFilter: String) { fundsByClient(clientId: $clientId, limit: $limit, offset: $offset, search: $search, statusFilter: $statusFilter) { id name code description type fundManager baseCurrency contactPerson contactEmail contactNumber sector geography strategy marketCap stage inceptionDate investmentStartDate commitmentSubscription isActive createdAt updatedAt fundAdmin shadow } }",
  "variables": {
    "clientId": 1,
    "limit": 10,
    "offset": 0,
    "search": null,
    "statusFilter": null
  }
}
```

**Response:**
```json
{
  "data": {
    "fundsByClient": [
      {
        "id": 1,
        "name": "Global Equity Fund",
        "code": "GEF001",
        "description": "A diversified global equity fund",
        "type": "Equity",
        "fundManager": "ABC Asset Management",
        "baseCurrency": "USD",
        "contactPerson": "John Doe",
        "contactEmail": "john.doe@example.com",
        "contactNumber": "+1-555-0123",
        "sector": "Technology",
        "geography": "Global",
        "strategy": {
          "strategies": ["Long/Short", "Market Neutral"]
        },
        "marketCap": "Large Cap",
        "stage": "Active",
        "inceptionDate": "2020-01-15",
        "investmentStartDate": "2020-02-01",
        "commitmentSubscription": 1000000.0,
        "isActive": true,
        "createdAt": "2020-01-10T10:00:00",
        "updatedAt": "2024-12-01T14:30:00"
      },
      {
        "id": 3,
        "name": "Private Equity Fund",
        "code": "PEF003",
        "description": "Private equity investments",
        "type": "Private Equity",
        "fundManager": "DEF Ventures",
        "baseCurrency": "USD",
        "contactPerson": "Bob Johnson",
        "contactEmail": "bob.johnson@example.com",
        "contactNumber": "+1-555-0789",
        "sector": "Private Equity",
        "geography": "North America",
        "strategy": null,
        "marketCap": null,
        "stage": "Active",
        "inceptionDate": "2018-03-10",
        "investmentStartDate": "2018-04-01",
        "commitmentSubscription": 25000000.0,
        "isActive": true,
        "createdAt": "2018-02-15T08:00:00",
        "updatedAt": "2024-10-20T11:15:00"
      }
    ]
  }
}
```

**Note:** This query uses the `client_funds` association table to find all funds linked to the specified client. If the client doesn't exist, an empty array is returned.

---

### 3. Search Funds with Filters

Example query with search and status filtering:

```json
{
  "query": "query SearchFunds { funds(search: \"Equity\", statusFilter: \"active\", limit: 5, offset: 0) { id name code type fundManager isActive } }"
}
```

**Response:**
```json
{
  "data": {
    "funds": [
      {
        "id": 1,
        "name": "Global Equity Fund",
        "code": "GEF001",
        "type": "Equity",
        "fundManager": "ABC Asset Management",
        "isActive": true
      }
    ]
  }
}
```

---

## Mutation Tests

> **Note:** When creating validations or ratios, you must provide at least one detail entry. The system creates:
> - One entry in `tbl_validation_master` (or `tbl_ratio_master`)
> - One or more entries in `tbl_validation_details` (or `tbl_ratio_details`)

> **Important:** Use GraphQL field names (converted from snake_case):
> - Note: The `tbl_ratio_details` table contains: `intratiodetailid`, `intratiomasterid`, `intdatamodelid`, `vcfilter`, `vcfiltertype`, `vcnumerator`, `vcdenominator`, and `vcformula`
>   - `vcnumerator`: Full CASE WHEN formula for numerator with Displayname prefix (e.g., "Displayname: Total Debt\n(SUM(CASE WHEN ...))")
>   - `vcdenominator`: Full CASE WHEN formula for denominator with Displayname prefix (e.g., "Displayname: Total Equity\n(SUM(CASE WHEN ...))")
>   - `vcformula`: Simplified formula using Numerator/Denominator placeholders (e.g., "((Period2.`Numerator`/Period2.`Denominator` - Period1.`Numerator`/Period1.`Denominator`)*100)/(Period1.`Numerator`/Period1.`Denominator`)")

### 10. Create Validation - Market Value

```json
{
  "query": "mutation CreateValidation($validationInput: ValidationMasterInput!, $detailsInput: [ValidationDetailsInput!]!) { createValidation(validationInput: $validationInput, detailsInput: $detailsInput) { success message validation { intvalidationmasterid intsubproductid vcvalidationname vctype vcsubtype vcsourcetype isactive } } }",
  "variables": {
    "validationInput": {
      "intsubproductid": 1,
      "vcsourcetype": "Single",
      "vctype": "P&L",
      "vcsubtype": "Market Value",
      "issubtypeSubtotal": false,
      "vcvalidationname": "Market Value Validation Test",
      "isvalidationSubtotal": false,
      "vcdescription": "Test validation for market values",
      "intthreshold": 0.05,
      "vcthresholdtype": "Percentage",
      "vcthresholdAbsRange": null,
      "intthresholdmin": null,
      "intthresholdmax": null,
      "intprecision": 4.0,
      "isactive": true
    },
    "detailsInput": [
      {
        "intdatamodelid": 1,
        "intgroupAttributeid": 10,
        "intassettypeid": 15,
        "intcalcAttributeid": 20,
        "vcaggregationtype": "sum",
        "vcfilter": "asset_class = 'Equity'",
        "vcfiltertype": "E",
        "vcformula": "SUM(market_value)"
      }
    ]
  }
}
```

**Response:**
```json
{
  "data": {
    "createValidation": {
      "success": true,
      "message": "Validation created successfully",
      "validation": {
        "intvalidationmasterid": 2,
        "intsubproductid": 1,
        "vcvalidationname": "Market Value Validation Test",
        "vctype": "P&L",
        "vcsubtype": "Market Value",
        "vcsourcetype": "Single",
        "isactive": true
      }
    }
  }
}
```

---

### 11. Create Validation - Positions

```json
{
  "query": "mutation CreateValidation($validationInput: ValidationMasterInput!, $detailsInput: [ValidationDetailsInput!]!) { createValidation(validationInput: $validationInput, detailsInput: $detailsInput) { success message validation { intvalidationmasterid vcvalidationname } } }",
  "variables": {
    "validationInput": {
      "intsubproductid": 1,
      "vcsourcetype": "Single",
      "vctype": "P&L",
      "vcsubtype": "Positions",
      "issubtypeSubtotal": false,
      "vcvalidationname": "Position Count Validation",
      "isvalidationSubtotal": false,
      "vcdescription": "Validates position counts match expected values",
      "intthreshold": 10,
      "vcthresholdtype": "Absolute",
      "vcthresholdAbsRange": "Range",
      "intthresholdmin": 5.0,
      "intthresholdmax": 15.0,
      "intprecision": 4.0,
      "isactive": true
    },
    "detailsInput": [
      {
        "intdatamodelid": 1,
        "intgroupAttributeid": 10,
        "intassettypeid": 15,
        "intcalcAttributeid": 20,
        "vcaggregationtype": "count",
        "vcfilter": null,
        "vcfiltertype": null,
        "vcformula": "COUNT(DISTINCT position_id)"
      }
    ]
  }
}
```

---

### 12. Create Validation - Dual Source

```json
{
  "query": "mutation CreateValidation($validationInput: ValidationMasterInput!, $detailsInput: [ValidationDetailsInput!]!) { createValidation(validationInput: $validationInput, detailsInput: $detailsInput) { success message validation { intvalidationmasterid vcvalidationname vcsourcetype } } }",
  "variables": {
    "validationInput": {
      "intsubproductid": 1,
      "vcsourcetype": "Dual",
      "vctype": "P&L",
      "vcsubtype": "Pricing",
      "issubtypeSubtotal": true,
      "vcvalidationname": "Dual Source Price Validation",
      "isvalidationSubtotal": true,
      "vcdescription": "Compare pricing from two sources",
      "intthreshold": 0.01,
      "vcthresholdtype": "Percentage",
      "intprecision": 4.0,
      "isactive": true
    },
    "detailsInput": [
      {
        "intdatamodelid": 1,
        "intgroupAttributeid": 13,
        "intassettypeid": 18,
        "intcalcAttributeid": 23,
        "vcaggregationtype": "avg",
        "vcfilter": "instrument_type = 'Bond'",
        "vcfiltertype": "I",
        "vcformula": "ABS(source1_price - source2_price) / source1_price"
      }
    ]
  }
}
```

**Response:**
```json
{
  "data": {
    "createValidation": {
      "success": true,
      "message": "Validation created successfully",
      "validation": {
        "intvalidationmasterid": 4,
        "vcvalidationname": "Dual Source Price Validation",
        "vcsourcetype": "Dual"
      }
    }
  }
}
```

---

### 13. Create Ratio - Financial (Debt to Equity)

```json
{
  "query": "mutation CreateRatio($ratioInput: RatioMasterInput!, $detailsInput: [RatioDetailsInput!]!) { createRatio(ratioInput: $ratioInput, detailsInput: $detailsInput) { success message ratio { intratiomasterid intsubproductid vcrationame vctype isactive vcsourcetype } } }",
  "variables": {
    "ratioInput": {
      "intsubproductid": 1,
      "vcsourcetype": "Single",
      "vctype": "Financial",
      "vcrationame": "Debt to Equity Ratio",
      "isratioSubtotal": false,
      "vcdescription": "Calculates debt to equity ratio for the fund",
      "intthreshold": 2.0,
      "vcthresholdtype": "Absolute",
      "intprecision": 4.0,
      "isactive": true
    },
    "detailsInput": [
      {
        "intdatamodelid": 1,
        "vcfilter": "asset_class = 'Debt'",
        "vcfiltertype": "I",
        "vcnumerator": "Displayname: Total Debt\n(SUM(CASE WHEN {'Trial Balance'}.[Account Type] = 'Liabilities' THEN ABS({'Trial Balance'}.[Closing Balance]) ELSE 0 END))",
        "vcdenominator": "Displayname: Total Equity\n(SUM(CASE WHEN {'Trial Balance'}.[Account Type] = 'Equity' THEN {'Trial Balance'}.[Closing Balance] ELSE 0 END))",
        "vcformula": "((Period2.`Numerator`/Period2.`Denominator` - Period1.`Numerator`/Period1.`Denominator`)*100)/(Period1.`Numerator`/Period1.`Denominator`)"
      }
    ]
  }
}
```

---

### 14. Create Ratio - Liquidity (Current Ratio)

```json
{
  "query": "mutation CreateRatio($ratioInput: RatioMasterInput!, $detailsInput: [RatioDetailsInput!]!) { createRatio(ratioInput: $ratioInput, detailsInput: $detailsInput) { success message ratio { intratiomasterid vcrationame } } }",
  "variables": {
    "ratioInput": {
      "intsubproductid": 1,
      "vcsourcetype": "Single",
      "vctype": "Liquidity",
      "vcrationame": "Current Ratio",
      "isratioSubtotal": false,
      "vcdescription": "Measures ability to pay short-term obligations",
      "intthreshold": 1.5,
      "vcthresholdtype": "Absolute",
      "intprecision": 4.0,
      "isactive": true
    },
    "detailsInput": [
      {
        "intdatamodelid": 2,
        "vcfilter": null,
        "vcfiltertype": null,
        "vcnumerator": "Displayname: Current Assets\n(SUM(CASE WHEN {'Trial Balance'}.[Account Type] = 'Assets' AND {'Trial Balance'}.[Sub Type] = 'Current' THEN {'Trial Balance'}.[Closing Balance] ELSE 0 END))",
        "vcdenominator": "Displayname: Current Liabilities\n(SUM(CASE WHEN {'Trial Balance'}.[Account Type] = 'Liabilities' AND {'Trial Balance'}.[Sub Type] = 'Current' THEN ABS({'Trial Balance'}.[Closing Balance]) ELSE 0 END))",
        "vcformula": "((Period2.`Numerator`/Period2.`Denominator` - Period1.`Numerator`/Period1.`Denominator`)*100)/(Period1.`Numerator`/Period1.`Denominator`)"
      }
    ]
  }
}
```

**Response:**
```json
{
  "data": {
    "createRatio": {
      "success": true,
      "message": "Ratio created successfully",
      "ratio": {
        "intratiomasterid": 2,
        "vcrationame": "Current Ratio"
      }
    }
  }
}
```

---

### 15. Create Ratio - Concentration

```json
{
  "query": "mutation CreateRatio($ratioInput: RatioMasterInput!, $detailsInput: [RatioDetailsInput!]!) { createRatio(ratioInput: $ratioInput, detailsInput: $detailsInput) { success message ratio { intratiomasterid vcrationame vctype } } }",
  "variables": {
    "ratioInput": {
      "intsubproductid": 1,
      "vcsourcetype": "Single",
      "vctype": "Concentration",
      "vcrationame": "Top 10 Holdings Concentration",
      "isratioSubtotal": true,
      "vcdescription": "Percentage of portfolio in top 10 holdings",
      "intthreshold": 0.40,
      "vcthresholdtype": "Percentage",
      "intprecision": 4.0,
      "isactive": true
    },
    "detailsInput": [
      {
        "intdatamodelid": 1,
        "vcfilter": "holding_rank <= 10",
        "vcfiltertype": "I",
        "vcnumerator": "Displayname: Top 10 Holdings Value\n(SUM(CASE WHEN {'Portfolio Valuation By Instrument'}.[Holding Rank] <= 10 THEN {'Portfolio Valuation By Instrument'}.[Market Value] ELSE 0 END))",
        "vcdenominator": "Displayname: Total Portfolio Value\n(SUM({'Portfolio Valuation By Instrument'}.[Market Value]))",
        "vcformula": "((Period2.`Numerator`/Period2.`Denominator` - Period1.`Numerator`/Period1.`Denominator`)*100)/(Period1.`Numerator`/Period1.`Denominator`)"
      }
    ]
  }
}
```

**Response:**
```json
{
  "data": {
    "createRatio": {
      "success": true,
      "message": "Ratio created successfully",
      "ratio": {
        "intratiomasterid": 3,
        "vcrationame": "Top 10 Holdings Concentration",
        "vctype": "Concentration"
      }
    }
  }
}
```

---

### 16. Create Ratio - Sentiment

```json
{
  "query": "mutation CreateRatio($ratioInput: RatioMasterInput!, $detailsInput: [RatioDetailsInput!]!) { createRatio(ratioInput: $ratioInput, detailsInput: $detailsInput) { success message ratio { intratiomasterid vcrationame } } }",
  "variables": {
    "ratioInput": {
      "intsubproductid": 1,
      "vcsourcetype": "Single",
      "vctype": "Sentiment",
      "vcrationame": "Net Long Position Ratio",
      "isratioSubtotal": false,
      "vcdescription": "Measures net long vs short positions",
      "intthreshold": 0.70,
      "vcthresholdtype": "Percentage",
      "intprecision": 4.0,
      "isactive": true
    },
    "detailsInput": [
      {
        "intdatamodelid": 1,
        "vcfilter": null,
        "vcfiltertype": null,
        "vcnumerator": "Displayname: Net Long Positions\n(SUM(CASE WHEN {'Portfolio Valuation By Instrument'}.[Position Type] = 'Long' THEN {'Portfolio Valuation By Instrument'}.[Market Value] ELSE 0 END) - SUM(CASE WHEN {'Portfolio Valuation By Instrument'}.[Position Type] = 'Short' THEN ABS({'Portfolio Valuation By Instrument'}.[Market Value]) ELSE 0 END))",
        "vcdenominator": "Displayname: Total Positions\n(SUM(ABS({'Portfolio Valuation By Instrument'}.[Market Value])))",
        "vcformula": "((Period2.`Numerator`/Period2.`Denominator` - Period1.`Numerator`/Period1.`Denominator`)*100)/(Period1.`Numerator`/Period1.`Denominator`)"
      }
    ]
  }
}
```

**Response:**
```json
{
  "data": {
    "createRatio": {
      "success": true,
      "message": "Ratio created successfully",
      "ratio": {
        "intratiomasterid": 4,
        "vcrationame": "Net Long Position Ratio"
      }
    }
  }
}
```

---

## Update Operations

> **Important:** Use `updateValidationComplete` (or `updateRatioComplete`) when editing from a form that shows both master fields and details. This single mutation updates everything together.
> 
> Use `updateValidation` (or `updateRatio`) only for simple master-only updates.

### 17. Update Validation - Change Details (Master Only)

Update only master fields without changing details:

```json
{
  "query": "mutation UpdateValidation($validationInput: UpdateValidationMasterInput!) { updateValidation(validationInput: $validationInput) { success message validation { intvalidationmasterid vcvalidationname vcsourcetype vcdescription intthreshold vcthresholdtype isactive intupdatedby dtupdatedat } } }",
  "variables": {
    "validationInput": {
      "intvalidationmasterid": 1,
      "vcvalidationname": "UPDATED: Market Value Validation",
      "vcdescription": "Updated description for market value validation",
      "intthreshold": 0.05,
      "vcsourcetype": "Single"
    }
  }
}
```

**Response:**
```json
{
  "data": {
    "updateValidation": {
      "success": true,
      "message": "Validation updated successfully",
      "validation": {
        "intvalidationmasterid": 1,
        "vcvalidationname": "UPDATED: Market Value Validation",
        "vcsourcetype": "Single",
        "vcdescription": "Updated description for market value validation",
        "intthreshold": 0.05,
        "vcthresholdtype": "Percentage",
        "isactive": true,
        "intupdatedby": 1,
        "dtupdatedat": "2025-10-27T18:55:00"
      }
    }
  }
}
```

---

### 18. Update Validation - Change Status Only

```json
{
  "query": "mutation UpdateValidation($validationInput: UpdateValidationMasterInput!) { updateValidation(validationInput: $validationInput) { success message validation { intvalidationmasterid vcvalidationname isactive dtupdatedat } } }",
  "variables": {
    "validationInput": {
      "intvalidationmasterid": 1,
      "isactive": false
    }
  }
}
```

**Response:**
```json
{
  "data": {
    "updateValidation": {
      "success": true,
      "message": "Validation updated successfully",
      "validation": {
        "intvalidationmasterid": 1,
        "vcvalidationname": "Market Value Validation",
        "isactive": false,
        "dtupdatedat": "2025-10-27T18:55:00"
      }
    }
  }
}
```

---

### 19. Update Validation - Change Threshold

```json
{
  "query": "mutation UpdateValidation($validationInput: UpdateValidationMasterInput!) { updateValidation(validationInput: $validationInput) { success message validation { intvalidationmasterid vcvalidationname intthreshold vcthresholdtype dtupdatedat } } }",
  "variables": {
    "validationInput": {
      "intvalidationmasterid": 2,
      "intthreshold": 15,
      "vcthresholdtype": "Absolute",
      "intprecision": 4.0
    }
  }
}
```

**Response:**
```json
{
  "data": {
    "updateValidation": {
      "success": true,
      "message": "Validation updated successfully",
      "validation": {
        "intvalidationmasterid": 2,
        "vcvalidationname": "Position Count Validation",
        "intthreshold": 15,
        "vcthresholdtype": "Absolute",
        "dtupdatedat": "2025-10-27T18:56:00"
      }
    }
  }
}
```

---

### 20. Update Ratio - Change Details

```json
{
  "query": "mutation UpdateRatio($ratioInput: UpdateRatioMasterInput!) { updateRatio(ratioInput: $ratioInput) { success message ratio { intratiomasterid vcrationame vcsourcetype vcdescription intthreshold vcthresholdtype isactive intupdatedby dtupdatedat } } }",
  "variables": {
    "ratioInput": {
      "intratiomasterid": 1,
      "vcrationame": "UPDATED: Debt to Equity Ratio",
      "vcdescription": "Updated measure of financial leverage",
      "intthreshold": 3.0,
      "vcsourcetype": "Dual"
    }
  }
}
```

**Response:**
```json
{
  "data": {
    "updateRatio": {
      "success": true,
      "message": "Ratio updated successfully",
      "ratio": {
        "intratiomasterid": 1,
        "vcrationame": "UPDATED: Debt to Equity Ratio",
        "vcsourcetype": "Dual",
        "vcdescription": "Updated measure of financial leverage",
        "intthreshold": 3.0,
        "vcthresholdtype": "Absolute",
        "isactive": true,
        "intupdatedby": 1,
        "dtupdatedat": "2025-10-27T18:57:00"
      }
    }
  }
}
```

---

### 21. Update Ratio - Change Status Only

```json
{
  "query": "mutation UpdateRatio($ratioInput: UpdateRatioMasterInput!) { updateRatio(ratioInput: $ratioInput) { success message ratio { intratiomasterid vcrationame isactive dtupdatedat } } }",
  "variables": {
    "ratioInput": {
      "intratiomasterid": 1,
      "isactive": true
    }
  }
}
```

**Response:**
```json
{
  "data": {
    "updateRatio": {
      "success": true,
      "message": "Ratio updated successfully",
      "ratio": {
        "intratiomasterid": 1,
        "vcrationame": "Debt to Equity Ratio",
        "isactive": true,
        "dtupdatedat": "2025-10-27T18:57:00"
      }
    }
  }
}
```

---

### 22. Update Ratio - Change Threshold

```json
{
  "query": "mutation UpdateRatio($ratioInput: UpdateRatioMasterInput!) { updateRatio(ratioInput: $ratioInput) { success message ratio { intratiomasterid vcrationame intthreshold vcthresholdtype dtupdatedat } } }",
  "variables": {
    "ratioInput": {
      "intratiomasterid": 2,
      "intthreshold": 2.5,
      "vcthresholdtype": "Absolute",
      "intprecision": 4.0
    }
  }
}
```

**Response:**
```json
{
  "data": {
    "updateRatio": {
      "success": true,
      "message": "Ratio updated successfully",
      "ratio": {
        "intratiomasterid": 2,
        "vcrationame": "Current Ratio",
        "intthreshold": 2.5,
        "vcthresholdtype": "Absolute",
        "dtupdatedat": "2025-10-27T18:58:00"
      }
    }
  }
}
```

---

## Complete Update Operations (Master + Details)

### 23. Update Validation Complete - Master and Details Together

Update master fields, modify existing details, add new details, and delete details all in one operation.

**Important:** 
- **`updateDetails`**: Items must have an `intvalidationdetailid` that already exists in the database. If the ID doesn't exist, those items are ignored.
- **`newDetails`**: These will always create new detail entries.
- **`deleteDetailIds`**: Array of detail IDs to delete.

```json
{
  "query": "mutation UpdateValidationComplete($input: UpdateValidationCompleteInput!) { updateValidationComplete(input: $input) { success message validation { intvalidationmasterid vcvalidationname vctype intthreshold vcthresholdtype isactive } } }",
  "variables": {
    "input": {
      "intvalidationmasterid": 1,
      "vcvalidationname": "Updated: Market Value Validation",
      "vcsourcetype": "Single",
      "vctype": "P&L",
      "vcsubtype": "Market Value",
      "issubtypeSubtotal": true,
      "vcdescription": "Updated description",
      "intthreshold": 0.10,
      "vcthresholdtype": "Percentage",
      "intprecision": 4.0,
      "isactive": true,
      "updateDetails": [
        {
          "intvalidationdetailid": 5,
          "intdatamodelid": 2,
          "intgroupAttributeid": 11,
          "intassettypeid": 16,
          "intcalcAttributeid": 21,
          "vcaggregationtype": "avg",
          "vcfilter": "asset_class IN ('Equity', 'Bond')",
          "vcfiltertype": "I",
          "vcformula": "AVG(market_value)"
        }
      ],
      "newDetails": [
        {
          "intdatamodelid": 1,
          "intgroupAttributeid": 12,
          "intassettypeid": 17,
          "intcalcAttributeid": 22,
          "vcaggregationtype": "max",
          "vcfilter": "market_value > 10000",
          "vcfiltertype": "I",
          "vcformula": "MAX(market_value)"
        }
      ],
      "deleteDetailIds": [6]
    }
  }
}
```

**Response:**
```json
{
  "data": {
    "updateValidationComplete": {
      "success": true,
      "message": "Validation updated successfully",
      "validation": {
        "intvalidationmasterid": 1,
        "vcvalidationname": "Updated: Market Value Validation",
        "vctype": "P&L",
        "intthreshold": 0.10,
        "vcthresholdtype": "Percentage",
        "isactive": true
      }
    }
  }
}
```

---

### 24. Update Ratio Complete - Master and Details Together

Update ratio master, modify existing details, add new details, and delete details all in one operation.

**Important:** 
- **`updateDetails`**: Items must have an `intratiodetailid` that already exists in the database. If the ID doesn't exist, those items are ignored.
- **`newDetails`**: These will always create new detail entries.
- **`deleteDetailIds`**: Array of detail IDs to delete.

```json
{
  "query": "mutation UpdateRatioComplete($input: UpdateRatioCompleteInput!) { updateRatioComplete(input: $input) { success message ratio { intratiomasterid vcrationame vctype intthreshold vcthresholdtype isactive } } }",
  "variables": {
    "input": {
      "intratiomasterid": 1,
      "vcrationame": "Updated: Debt to Equity Ratio",
      "vcsourcetype": "Dual",
      "vctype": "Financial",
      "isratioSubtotal": true,
      "vcdescription": "Updated financial ratio description",
      "intthreshold": 2.5,
      "vcthresholdtype": "Absolute",
      "intprecision": 4.0,
      "isactive": true,
      "updateDetails": [
        {
          "intratiodetailid": 3,
          "intdatamodelid": 2,
          "vcfilter": "asset_type = 'Investment'",
          "vcfiltertype": "I",
          "vcnumerator": "Displayname: Total Debt\n(SUM(CASE WHEN {'Trial Balance'}.[Account Type] = 'Liabilities' THEN ABS({'Trial Balance'}.[Closing Balance]) ELSE 0 END))",
          "vcdenominator": "Displayname: Total Equity\n(SUM(CASE WHEN {'Trial Balance'}.[Account Type] = 'Equity' THEN {'Trial Balance'}.[Closing Balance] ELSE 0 END))",
          "vcformula": "((Period2.`Numerator`/Period2.`Denominator` - Period1.`Numerator`/Period1.`Denominator`)*100)/(Period1.`Numerator`/Period1.`Denominator`)"
        }
      ],
      "newDetails": [
        {
          "intdatamodelid": 1,
          "vcfilter": null,
          "vcfiltertype": null,
          "vcnumerator": "Displayname: Total Assets\n(SUM(CASE WHEN {'Trial Balance'}.[Account Type] = 'Assets' THEN {'Trial Balance'}.[Closing Balance] ELSE 0 END))",
          "vcdenominator": "Displayname: Total Liabilities\n(SUM(CASE WHEN {'Trial Balance'}.[Account Type] = 'Liabilities' THEN ABS({'Trial Balance'}.[Closing Balance]) ELSE 0 END))",
          "vcformula": "((Period2.`Numerator`/Period2.`Denominator` - Period1.`Numerator`/Period1.`Denominator`)*100)/(Period1.`Numerator`/Period1.`Denominator`)"
        }
      ],
      "deleteDetailIds": [4]
    }
  }
}
```

**Response:**
```json
{
  "data": {
    "updateRatioComplete": {
      "success": true,
      "message": "Ratio updated successfully",
      "ratio": {
        "intratiomasterid": 1,
        "vcrationame": "Updated: Debt to Equity Ratio",
        "vctype": "Financial",
        "intthreshold": 2.5,
        "vcthresholdtype": "Absolute",
        "isactive": true
      }
    }
  }
}
```

---

## Delete Operations

### 25. Delete Validation Master and Details

Delete a validation master and all its associated details:

```json
{
  "query": "mutation DeleteValidation($intvalidationmasterid: Int!) { deleteValidation(intvalidationmasterid: $intvalidationmasterid) { success message } }",
  "variables": {
    "intvalidationmasterid": 1
  }
}
```

**Response:**
```json
{
  "data": {
    "deleteValidation": {
      "success": true,
      "message": "Validation 1 and all its details deleted successfully"
    }
  }
}
```

---

### 26. Delete Ratio Master and Details

Delete a ratio master and all its associated details:

```json
{
  "query": "mutation DeleteRatio($intratiomasterid: Int!) { deleteRatio(intratiomasterid: $intratiomasterid) { success message } }",
  "variables": {
    "intratiomasterid": 1
  }
}
```

**Response:**
```json
{
  "data": {
    "deleteRatio": {
      "success": true,
      "message": "Ratio 1 and all its details deleted successfully"
    }
  }
}
```

---

## Testing Tips

### 1. Authentication
First, get a JWT token from your login endpoint, then use it in all requests:
```
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

### 2. Testing Sequence
Recommended order:
1. Test queries first (1-9) to see existing data
2. Test mutations (10-16) to create new data
3. Re-run queries to verify the created data
4. Test update operations (17-24) to modify data
5. Test delete operations (25-26) to remove data

### 3. Environment Variables
In Postman, create an environment with:
- `base_url`: `http://localhost:8000` (or your server URL)
- `jwt_token`: Your actual JWT token

Then use:
```
URL: {{base_url}}/graphql
Authorization: Bearer {{jwt_token}}
```

### 4. Filtering on Client Side
All queries return the `isactive` field. Filter inactive records in your frontend:
```javascript
const activeSubproducts = data.getSubproducts.filter(sp => sp.isactive);
```

### 5. Testing Different Subproducts
Change the `intsubproductid` variable to test different subproducts:
```json
{
  "variables": {
    "intsubproductid": 2
  }
}
```

---

## Expected Response Format

### Successful Query Response
```json
{
  "data": {
    "getSubproducts": [
      {
        "intsubproductid": 1,
        "vcsubproductname": "NAV",
        "isactive": true,
        ...
      }
    ]
  }
}
```

### Successful Mutation Response
```json
{
  "data": {
    "createValidation": {
      "success": true,
      "message": "Validation created successfully",
      "validation": {
        "intvalidationmasterid": 1,
        "vcvalidationname": "Market Value Validation Test",
        ...
      }
    }
  }
}
```

### Error Response
```json
{
  "errors": [
    {
      "message": "Authentication required",
      "path": ["getSubproducts"]
    }
  ]
}
```

---

## Data Model Column APIs (for Validation/Ratio Attribute & Asset Type Dropdowns)

### 23. Get All Data Models

Get all data models available in the system for selection.

```json
{
  "query": "query GetAllDataModels { getDataModels { intdatamodelid vcmodelname vcdescription } }"
}
```

**Response:**
```json
{
  "data": {
    "getDataModels": [
      {
        "intdatamodelid": 1,
        "vcmodelname": "Trial Balance",
        "vcdescription": "Trial balance data model"
      },
      {
        "intdatamodelid": 2,
        "vcmodelname": "Portfolio Valuation",
        "vcdescription": "Portfolio valuation data model"
      }
    ]
  }
}
```

---

### 24. Get Data Model Columns for Attribute/Asset Type Dropdown

Get columns for a specific data model to populate Attribute and Asset Type dropdowns.

**Query:**
```json
{
  "query": "query GetDataModelColumns($intdatamodelid: Int!) { getDataModelColumns(intdatamodelid: $intdatamodelid) { intdatamodeldetailid intdatamodelid vcfieldname vcfielddescription vcdatatype intlength intprecision intscale vcdateformat vcdmcolumnname vcdefaultvalue ismandatory intdisplayorder } }",
  "variables": {
    "intdatamodelid": 1
  }
}
```

**Response:**
```json
{
  "data": {
    "getDataModelColumns": [
      {
        "intdatamodeldetailid": 1,
        "intdatamodelid": 1,
        "vcfieldname": "Account Description",
        "vcfielddescription": "Description of the account",
        "vcdatatype": "Text",
        "intlength": 255,
        "intprecision": null,
        "intscale": null,
        "vcdateformat": null,
        "vcdmcolumnname": "accountdescription",
        "vcdefaultvalue": null,
        "ismandatory": true,
        "intdisplayorder": 1
      },
      {
        "intdatamodeldetailid": 2,
        "intdatamodelid": 1,
        "vcfieldname": "Account Type",
        "vcfielddescription": "Type of account",
        "vcdatatype": "Text",
        "intlength": 50,
        "intprecision": null,
        "intscale": null,
        "vcdateformat": null,
        "vcdmcolumnname": "accounttype",
        "vcdefaultvalue": null,
        "ismandatory": false,
        "intdisplayorder": 2
      },
      {
        "intdatamodeldetailid": 3,
        "intdatamodelid": 1,
        "vcfieldname": "Ending Balance",
        "vcfielddescription": "Ending balance amount",
        "vcdatatype": "Numeric(Decimal)",
        "intlength": null,
        "intprecision": 15,
        "intscale": 2,
        "vcdateformat": null,
        "vcdmcolumnname": "endingbalance",
        "vcdefaultvalue": "0.00",
        "ismandatory": true,
        "intdisplayorder": 3
      }
    ]
  }
}
```

**Usage Notes:**
- Use this query when the user selects a "Data Model" in the validation/ratio specification table
- The returned columns populate both "Attribute" and "Asset Type" dropdowns
- Columns are returned ordered by `intdisplayorder`
- The `vccolumnname` field should be displayed to the user in the dropdown
- The `intmodeldetailid` is the value saved to `tbl_validation_details.intattribute` or `tbl_validation_details.intassettype`

---

## Data Model & Table Schema APIs

> **Note on Auto-Generation:** 
> 
> **Table Names (`vctablename`):** Automatically generated from `vcmodelname` if not provided:
> - Converts to lowercase
> - Replaces all special characters with underscores
> - Collapses multiple underscores into one
> - Adds `dm_` prefix
> 
> **Examples:**
> - `"Customer Management"` → `dm_customer_management`
> - `"Receivable-and.Payable Journal"` → `dm_receivable_and_payable_journal`
> - `"Product/Catalog-V2"` → `dm_product_catalog_v2`
> 
> **Column Names (`vcdmcolumnname`):** Automatically generated from `vcfieldname` if not provided:
> - Converts to lowercase
> - Removes all spaces and special characters
> - Just concatenates everything (no underscores)
> 
> **Examples:**
> - `"Customer ID"` → `customerid`
> - `"Email Address"` → `emailaddress`
> - `"Registration Date"` → `registrationdate`
> - `"Unit Price"` → `unitprice`
> 
> You can override both by providing the values explicitly.

### 25. Get All Data Models (Full Details)

```json
{
  "query": "query GetAllDataModels { dataModels1 { intdatamodelid vcmodelname vcdescription vcmodelid vccategory vcsource vctablename dtcreatedat intcreatedby dtupdatedat intupdatedby } }"
}
```

---

### 26. Get Data Models by Client ID

```json
{
  "query": "query GetDataModelsByClient($intclientid: Int) { dataModels(intclientid: $intclientid) { intdatamodelid vcmodelname vcdescription vcmodelid vccategory vcsource vctablename dtcreatedat intcreatedby dtupdatedat intupdatedby } }",
  "variables": {
    "intclientid": 1
  }
}
```

---

### 27. Get Data Models by Name (Search)

```json
{
  "query": "query SearchDataModels($vcmodelname: String) { dataModels(vcmodelname: $vcmodelname) { intdatamodelid vcmodelname vcdescription vcmodelid vccategory vcsource vctablename dtcreatedat intcreatedby dtupdatedat intupdatedby } }",
  "variables": {
    "vcmodelname": "Customer"
  }
}
```

---

### 27a. Get All Data Models with Pagination

**Note:** The `getAllDataModels` query returns a paginated response with `dataModels` list and `pagination` information.

#### 27a.1. Get All Data Models (Default Pagination)

Uses default page 1 with 20 items per page:

```json
{
  "query": "query GetAllDataModels { getAllDataModels { dataModels { intdatamodelid vcmodelname vcdescription vcmodelid vccategory vcsource vctablename isactive fieldCount dtcreatedat intcreatedby dtupdatedat intupdatedby } pagination { pageNumber pageSize currentPage totalPages totalCount } } }"
}
```

**Response:**
```json
{
  "data": {
    "getAllDataModels": {
      "dataModels": [
        {
          "intdatamodelid": 1,
          "vcmodelname": "Customer Management",
          "vcdescription": "Customer data model for CRM system",
          "vcmodelid": "CUST_001",
          "vccategory": "CRM",
          "vcsource": "Manual",
          "vctablename": "dm_customer_management",
          "isactive": true,
          "fieldCount": 5,
          "dtcreatedat": "2025-01-15T10:30:00",
          "intcreatedby": 1,
          "dtupdatedat": "2025-01-20T14:45:00",
          "intupdatedby": 1
        },
        {
          "intdatamodelid": 2,
          "vcmodelname": "Product Catalog",
          "vcdescription": "Product information and pricing",
          "vcmodelid": "PROD_001",
          "vccategory": "Inventory",
          "vcsource": "Manual",
          "vctablename": "dm_product_catalog",
          "isactive": true,
          "fieldCount": 8,
          "dtcreatedat": "2025-01-16T09:00:00",
          "intcreatedby": 1,
          "dtupdatedat": null,
          "intupdatedby": null
        }
      ],
      "pagination": {
        "pageNumber": 1,
        "pageSize": 20,
        "currentPage": 1,
        "totalPages": 3,
        "totalCount": 45
      }
    }
  }
}
```

#### 27a.2. Get Data Models with Custom Pagination

Specify page number and page size:

```json
{
  "query": "query GetAllDataModels($pageNumber: Int, $pageSize: Int) { getAllDataModels(pageNumber: $pageNumber, pageSize: $pageSize) { dataModels { intdatamodelid vcmodelname vcdescription vcmodelid vccategory vcsource vctablename isactive fieldCount } pagination { pageNumber pageSize currentPage totalPages totalCount } } }",
  "variables": {
    "pageNumber": 2,
    "pageSize": 10
  }
}
```

**Response:**
```json
{
  "data": {
    "getAllDataModels": {
      "dataModels": [
        {
          "intdatamodelid": 11,
          "vcmodelname": "Order Management",
          "vcdescription": "Order tracking and management",
          "vcmodelid": "ORD_001",
          "vccategory": "Sales",
          "vcsource": "Manual",
          "vctablename": "dm_order_management",
          "isactive": true,
          "fieldCount": 12
        }
      ],
      "pagination": {
        "pageNumber": 2,
        "pageSize": 10,
        "currentPage": 2,
        "totalPages": 5,
        "totalCount": 45
      }
    }
  }
}
```

#### 27a.3. Get Data Models by Client ID with Pagination

Filter by client ID and paginate:

```json
{
  "query": "query GetDataModelsByClient($intclientid: Int, $pageNumber: Int, $pageSize: Int) { getAllDataModels(intclientid: $intclientid, pageNumber: $pageNumber, pageSize: $pageSize) { dataModels { intdatamodelid vcmodelname vcdescription vcmodelid vccategory vcsource vctablename isactive fieldCount } pagination { pageNumber pageSize currentPage totalPages totalCount } } }",
  "variables": {
    "intclientid": 1,
    "pageNumber": 1,
    "pageSize": 20
  }
}
```

**Response:**
```json
{
  "data": {
    "getAllDataModels": {
      "dataModels": [
        {
          "intdatamodelid": 1,
          "vcmodelname": "Customer Management",
          "vcdescription": "Customer data model for CRM system",
          "vcmodelid": "CUST_001",
          "vccategory": "CRM",
          "vcsource": "Manual",
          "vctablename": "dm_customer_management",
          "isactive": true,
          "fieldCount": 5
        }
      ],
      "pagination": {
        "pageNumber": 1,
        "pageSize": 20,
        "currentPage": 1,
        "totalPages": 1,
        "totalCount": 12
      }
    }
  }
}
```

#### 27a.4. Search Data Models by Name with Pagination

Filter by model name (partial match) and paginate:

```json
{
  "query": "query SearchDataModels($vcmodelname: String, $pageNumber: Int, $pageSize: Int) { getAllDataModels(vcmodelname: $vcmodelname, pageNumber: $pageNumber, pageSize: $pageSize) { dataModels { intdatamodelid vcmodelname vcdescription vcmodelid vccategory vcsource vctablename isactive fieldCount } pagination { pageNumber pageSize currentPage totalPages totalCount } } }",
  "variables": {
    "vcmodelname": "Customer",
    "pageNumber": 1,
    "pageSize": 10
  }
}
```

**Response:**
```json
{
  "data": {
    "getAllDataModels": {
      "dataModels": [
        {
          "intdatamodelid": 1,
          "vcmodelname": "Customer Management",
          "vcdescription": "Customer data model for CRM system",
          "vcmodelid": "CUST_001",
          "vccategory": "CRM",
          "vcsource": "Manual",
          "vctablename": "dm_customer_management",
          "isactive": true,
          "fieldCount": 5
        }
      ],
      "pagination": {
        "pageNumber": 1,
        "pageSize": 10,
        "currentPage": 1,
        "totalPages": 1,
        "totalCount": 3
      }
    }
  }
}
```

#### 27a.5. Get Data Models with Combined Filters and Pagination

Filter by both client ID and model name with pagination:

```json
{
  "query": "query GetDataModelsFiltered($intclientid: Int, $vcmodelname: String, $pageNumber: Int, $pageSize: Int) { getAllDataModels(intclientid: $intclientid, vcmodelname: $vcmodelname, pageNumber: $pageNumber, pageSize: $pageSize) { dataModels { intdatamodelid vcmodelname vcdescription vcmodelid vccategory vcsource vctablename isactive fieldCount dtcreatedat } pagination { pageNumber pageSize currentPage totalPages totalCount } } }",
  "variables": {
    "intclientid": 1,
    "vcmodelname": "Product",
    "pageNumber": 1,
    "pageSize": 20
  }
}
```

**Response:**
```json
{
  "data": {
    "getAllDataModels": {
      "dataModels": [
        {
          "intdatamodelid": 2,
          "vcmodelname": "Product Catalog",
          "vcdescription": "Product information and pricing",
          "vcmodelid": "PROD_001",
          "vccategory": "Inventory",
          "vcsource": "Manual",
          "vctablename": "dm_product_catalog",
          "isactive": true,
          "fieldCount": 8,
          "dtcreatedat": "2025-01-16T09:00:00"
        }
      ],
      "pagination": {
        "pageNumber": 1,
        "pageSize": 20,
        "currentPage": 1,
        "totalPages": 1,
        "totalCount": 1
      }
    }
  }
}
```

---

### 28. Get Data Model Details with Columns

```json
{
  "query": "query GetDataModelDetails($intdatamodelid: Int!) { dataModelDetails(intdatamodelid: $intdatamodelid) { intdatamodelid vcmodelname vcdescription isactive columns { intdatamodeldetailid intdatamodelid vcfieldname vcfielddescription vcdatatype intlength intprecision intscale vcdateformat vcdmcolumnname vcdefaultvalue ismandatory intdisplayorder intcreatedby dtcreatedat intupdatedby dtupdatedat } } }",
  "variables": {
    "intdatamodelid": 1
  }
}
```

---

### 29. Create Data Model - Customer Table

```json
{
  "query": "mutation CreateDataModel($master: DataModelMasterInput!, $columns: [DataModelColumnInput!]!) { createDataModel(master: $master, columns: $columns) { intdatamodelid vcmodelname vcdescription columns { intdatamodeldetailid vcfieldname vcdmcolumnname intlength intprecision intscale vcdateformat vcdefaultvalue intdisplayorder } } }",
  "variables": {
    "master": {
      "vcmodelname": "Customer Management",
      "vcdescription": "Customer data model for CRM system",
      "vcmodelid": "CUST_001",
      "vccategory": "CRM",
      "vcsource": "Manual"
      // Note: vctablename will be auto-generated as "dm_customer_management" if not provided
      // You can override by providing "vctablename": "custom_table_name"
    },
    "columns": [
      {
        "vcfieldname": "Customer ID",
        "vcfielddescription": "Unique customer identifier",
        // vcdmcolumnname will be auto-generated as "customerid" if not provided
        "intlength": 10,
        "intdisplayorder": 1
      },
      {
        "vcfieldname": "Customer Name",
        "vcfielddescription": "Full name of the customer",
        // vcdmcolumnname will be auto-generated as "customername" if not provided
        "intlength": 255,
        "intdisplayorder": 2
      },
      {
        "vcfieldname": "Email Address",
        "vcfielddescription": "Customer email address",
        // vcdmcolumnname will be auto-generated as "emailaddress" if not provided
        "intlength": 100,
        "intdisplayorder": 3
      },
      {
        "vcfieldname": "Registration Date",
        "vcfielddescription": "Date when customer registered",
        // vcdmcolumnname will be auto-generated as "registrationdate" if not provided
        "vcdateformat": "YYYY-MM-DD",
        "intdisplayorder": 4
      },
      {
        "vcfieldname": "Account Balance",
        "vcfielddescription": "Current account balance",
        // vcdmcolumnname will be auto-generated as "accountbalance" if not provided
        "intprecision": 12,
        "intscale": 2,
        "vcdefaultvalue": "0.00",
        "intdisplayorder": 5
      }
    ]
  }
}
```

---

### 30. Create Data Model - Product Table

```json
{
  "query": "mutation CreateDataModel($master: DataModelMasterInput!, $columns: [DataModelColumnInput!]!) { createDataModel(master: $master, columns: $columns) { intdatamodelid vcmodelname vcdescription columns { intdatamodeldetailid vcfieldname vcdmcolumnname intlength intprecision intscale vcdateformat vcdefaultvalue intdisplayorder } } }",
  "variables": {
    "master": {
      "vcmodelname": "Product Catalog",
      "vcdescription": "Product information and pricing",
      "vcmodelid": "PROD_001",
      "vccategory": "Inventory",
      "vcsource": "Manual"
      // Note: vctablename will be auto-generated as "dm_product_catalog" if not provided
    },
    "columns": [
      {
        "vcfieldname": "Product ID",
        "vcfielddescription": "Unique product identifier",
        // vcdmcolumnname will be auto-generated as "productid" if not provided
        "intdisplayorder": 1
      },
      {
        "vcfieldname": "Product Name",
        "vcfielddescription": "Name of the product",
        // vcdmcolumnname will be auto-generated as "productname" if not provided
        "intlength": 200,
        "intdisplayorder": 2
      },
      {
        "vcfieldname": "Category",
        "vcfielddescription": "Product category",
        // vcdmcolumnname will be auto-generated as "category" if not provided
        "intlength": 50,
        "intdisplayorder": 3
      },
      {
        "vcfieldname": "Unit Price",
        "vcfielddescription": "Price per unit",
        // vcdmcolumnname will be auto-generated as "unitprice" if not provided
        "intprecision": 10,
        "intscale": 2,
        "intdisplayorder": 4
      },
      {
        "vcfieldname": "Stock Quantity",
        "vcfielddescription": "Number of items in stock",
        // vcdmcolumnname will be auto-generated as "stockquantity" if not provided
        "vcdefaultvalue": "0",
        "intdisplayorder": 5
      }
    ]
  }
}
```

---

### 31. Create Physical Table from Data Model

```json
{
  "query": "mutation CreatePhysicalTable($intdatamodelid: Int!) { createPhysicalTable(intdatamodelid: $intdatamodelid) { success message tableName schemaName sqlStatement } }",
  "variables": {
    "intmodelid": 1
  }
}
```

---

### 32. Create Physical Table - Product Table

```json
{
  "query": "mutation CreatePhysicalTable($intdatamodelid: Int!) { createPhysicalTable(intdatamodelid: $intdatamodelid) { success message tableName schemaName sqlStatement } }",
  "variables": {
    "intmodelid": 2
  }
}
```

---

### 33. Update Data Model Complete - Master and Columns Together

Update data model master, modify existing columns, add new columns, and delete columns all in one operation.

**Important:** 
- **`updateColumns`**: Items must have an `intdatamodeldetailid` that already exists in the database. If the ID doesn't exist, those items are ignored.
- **`newColumns`**: These will always create new column entries.
- **`deleteColumnIds`**: Array of column IDs to delete.
- **Auto-update Rule**: If `vcmodelname` is updated and `vctablename` is not explicitly provided, `vctablename` will be automatically regenerated from the new model name using the same rules as creation.

**Data Type Change Restrictions:**
The following datatype changes are **NOT ALLOWED**:
- **Text/Alphanumeric** → Any other datatype (NO)
- **Date** → Any other datatype except Text (NO)
- **Boolean** → Any other datatype (NO)
- **Any datatype** → Date (NO)
- **Any datatype** → Boolean (NO)

**Allowed datatype changes:**
- **Number (Integer/Decimal)** → Text ✓
- **Date** → Text ✓

**Precision Cleanup:**
When a datatype is changed, the precision/scale values of the old datatype are automatically cleared:
- When changing from **Number to Text**: `intprecision` and `intscale` are set to `null`
- When changing from **Date to Text**: `vcdateformat` is set to `null`
- When changing from **Text to Number**: `intlength` is cleared (if applicable)

```json
{
  "query": "mutation UpdateDataModelComplete($input: UpdateDataModelCompleteInput!) { updateDataModelComplete(input: $input) { intdatamodelid vcmodelname vcdescription columns { intdatamodeldetailid vcfieldname vcdmcolumnname vcdatatype intlength intprecision intscale vcdefaultvalue ismandatory intdisplayorder } } }",
  "variables": {
    "input": {
      "intdatamodelid": 1,
      "vcmodelname": "Updated Customer Management",
      "vcdescription": "Updated customer data model description",
      "updateColumns": [
        {
          "intdatamodeldetailid": 1,
          "vcfieldname": "Updated Customer ID",
          "vcfielddescription": "Updated customer identifier",
          "vcdatatype": "Text",
          "intlength": 20
        }
      ],
      "newColumns": [
        {
          "vcfieldname": "Phone Number",
          "vcfielddescription": "Customer phone number",
          "vcdatatype": "Text",
          "intlength": 15,
          "intdisplayorder": 6
        }
      ],
      "deleteColumnIds": [5]
    }
  }
}
```

**Response:**
```json
{
  "data": {
    "updateDataModelComplete": {
      "intdatamodelid": 1,
      "vcmodelname": "Updated Customer Management",
      "vcdescription": "Updated customer data model description",
      "vctablename": "dm_updated_customer_management",
      "columns": [
        {
          "intdatamodeldetailid": 1,
          "vcfieldname": "Updated Customer ID",
          "vcdmcolumnname": "customerid",
          "vcdatatype": "Text",
          "intlength": 20,
          "intprecision": null,
          "intscale": null,
          "vcdefaultvalue": null,
          "ismandatory": true,
          "intdisplayorder": 1
        },
        {
          "intdatamodeldetailid": 10,
          "vcfieldname": "Phone Number",
          "vcdmcolumnname": "phonenumber",
          "vcdatatype": "Text",
          "intlength": 15,
          "intprecision": null,
          "intscale": null,
          "vcdefaultvalue": null,
          "ismandatory": false,
          "intdisplayorder": 6
        }
      ]
    }
  }
}
```

---

### 34. Update Data Model - Auto-update Table Name Example

When updating the model name without specifying the table name, it will be automatically regenerated:

```json
{
  "query": "mutation UpdateDataModelComplete($input: UpdateDataModelCompleteInput!) { updateDataModelComplete(input: $input) { intdatamodelid vcmodelname vcdescription } }",
  "variables": {
    "input": {
      "intdatamodelid": 2,
      "vcmodelname": "Product Inventory System",
      "vcdescription": "Updated product inventory model"
    }
  }
}
```

**Response:**
```json
{
  "data": {
    "updateDataModelComplete": {
      "intdatamodelid": 2,
      "vcmodelname": "Product Inventory System",
      "vcdescription": "Updated product inventory model"
    }
  }
}
```

**Note:** Since `vctablename` was not provided in the input, it was automatically generated as `dm_product_inventory_system` from the model name `Product Inventory System`. However, `vctablename` is not returned in the response (only available in `DataModelMasterType`, not `DataModelDetailType`). Use the `dataModels` query to retrieve `vctablename` if needed.

---

### 35. Update Data Model - Override Table Name Example

You can also explicitly provide the table name to override the auto-generation:

```json
{
  "query": "mutation UpdateDataModelComplete($input: UpdateDataModelCompleteInput!) { updateDataModelComplete(input: $input) { intdatamodelid vcmodelname vcdescription } }",
  "variables": {
    "input": {
      "intdatamodelid": 2,
      "vcmodelname": "Product Inventory System",
      "vctablename": "custom_table_name"
    }
  }
}
```

**Response:**
```json
{
  "data": {
    "updateDataModelComplete": {
      "intdatamodelid": 2,
      "vcmodelname": "Product Inventory System",
      "vcdescription": null
    }
  }
}
```

**Note:** Since `vctablename` was explicitly provided in the input, it was used as-is instead of auto-generating. However, `vctablename` is not returned in the response (only available in `DataModelMasterType`, not `DataModelDetailType`). Use the `dataModels` query to retrieve `vctablename` if needed.

---

### 36. Delete Data Model (Admin Only)

```json
{
  "query": "mutation DeleteDataModel($intdatamodelid: Int!) { deleteDataModel(intdatamodelid: $intdatamodelid) }",
  "variables": {
    "intdatamodelid": 1
  }
}
```

---

## Configuration Tables API Examples

### 33. Get Validation Configurations (with Pagination)

```json
{
  "query": "query GetValidationConfigurations($pageNumber: Int, $pageSize: Int, $intvalidationmasterid: Int, $intclientid: Int, $intfundid: Int) { getValidationConfigurations(pageNumber: $pageNumber, pageSize: $pageSize, intvalidationmasterid: $intvalidationmasterid, intclientid: $intclientid, intfundid: $intfundid) { validationconfigurations { intvalidationconfigurationid intclientid intfundid intvalidationmasterid isactive vccondition intthreshold vcthresholdtype intprecision intcreatedby dtcreatedat intupdatedby dtupdatedat subproduct { intsubproductid vcsubproductname } client { id name } fund { id name } vcvalidationname } pagination { pageNumber pageSize currentPage totalPages totalCount } } }",
  "variables": {
    "pageNumber": 1,
    "pageSize": 10,
    "intvalidationmasterid": null,
    "intclientid": null,
    "intfundid": null
  }
}
```

**Response:**
```json
{
  "data": {
    "getValidationConfigurations": {
      "validationconfigurations": [
        {
          "intvalidationconfigurationid": 1,
          "intclientid": 1,
          "intfundid": 1,
          "intvalidationmasterid": 1,
          "isactive": true,
          "vccondition": ">=",
          "intthreshold": 100.0,
          "vcthresholdtype": "Absolute",
          "intprecision": 2.0,
          "intcreatedby": 1,
          "dtcreatedat": "2025-01-28T10:00:00",
          "intupdatedby": null,
          "dtupdatedat": null,
          "subproduct": {
            "intsubproductid": 1,
            "vcsubproductname": "NAV"
          },
          "client": {
            "id": 1,
            "name": "Example Client"
          },
          "fund": {
            "id": 1,
            "name": "Example Fund"
          },
          "vcvalidationname": "Position Count Validation"
        }
      ],
      "pagination": {
        "pageNumber": 1,
        "pageSize": 10,
        "currentPage": 1,
        "totalPages": 1,
        "totalCount": 1
      }
    }
  }
}
```

---

### 34. Get Ratio Configurations (with Pagination)

```json
{
  "query": "query GetRatioConfigurations($pageNumber: Int, $pageSize: Int, $intratiomasterid: Int, $intclientid: Int, $intfundid: Int) { getRatioConfigurations(pageNumber: $pageNumber, pageSize: $pageSize, intratiomasterid: $intratiomasterid, intclientid: $intclientid, intfundid: $intfundid) { ratioconfigurations { intratioconfigurationid intclientid intfundid intratiomasterid isactive vccondition intthreshold vcthresholdtype intprecision intcreatedby dtcreatedat intupdatedby dtupdatedat subproduct { intsubproductid vcsubproductname } client { id name } fund { id name } vcrationame } pagination { pageNumber pageSize currentPage totalPages totalCount } } }",
  "variables": {
    "pageNumber": 1,
    "pageSize": 10,
    "intratiomasterid": null,
    "intclientid": null,
    "intfundid": null
  }
}
```

**Response:**
```json
{
  "data": {
    "getRatioConfigurations": {
      "ratioconfigurations": [
        {
          "intratioconfigurationid": 1,
          "intclientid": 1,
          "intfundid": 1,
          "intratiomasterid": 1,
          "isactive": true,
          "vccondition": ">=",
          "intthreshold": 1.5,
          "vcthresholdtype": "Absolute",
          "intprecision": 2.0,
          "intcreatedby": 1,
          "dtcreatedat": "2025-01-28T10:00:00",
          "intupdatedby": null,
          "dtupdatedat": null,
          "subproduct": {
            "intsubproductid": 1,
            "vcsubproductname": "NAV"
          },
          "client": {
            "id": 1,
            "name": "Example Client"
          },
          "fund": {
            "id": 1,
            "name": "Example Fund"
          },
          "vcrationame": "Debt to Equity Ratio"
        }
      ],
      "pagination": {
        "pageNumber": 1,
        "pageSize": 10,
        "currentPage": 1,
        "totalPages": 1,
        "totalCount": 1
      }
    }
  }
}
```

---

### 35. Create Validation Configuration

```json
{
  "query": "mutation CreateValidationConfiguration($input: ValidationConfigurationInput!) { createValidationConfiguration(input: $input) { success message validationconfiguration { intvalidationconfigurationid intclientid intfundid intvalidationmasterid isactive vccondition intthreshold vcthresholdtype intprecision } } }",
  "variables": {
    "input": {
      "intclientid": 1,
      "intfundid": 1,
      "intvalidationmasterid": 1,
      "isactive": true,
      "vccondition": ">=",
      "intthreshold": 100.0,
      "vcthresholdtype": "Absolute",
      "intprecision": 2.0
    }
  }
}
```

**Response:**
```json
{
  "data": {
    "createValidationConfiguration": {
      "success": true,
      "message": "Validation configuration created successfully",
      "validationconfiguration": {
        "intvalidationconfigurationid": 1,
        "intclientid": 1,
        "intfundid": 1,
        "intvalidationmasterid": 1,
        "isactive": true,
        "vccondition": ">=",
        "intthreshold": 100.0,
        "vcthresholdtype": "Absolute",
        "intprecision": 2.0
      }
    }
  }
}
```

---

### 36. Create Ratio Configuration

```json
{
  "query": "mutation CreateRatioConfiguration($input: RatioConfigurationInput!) { createRatioConfiguration(input: $input) { success message ratioconfiguration { intratioconfigurationid intclientid intfundid intratiomasterid isactive vccondition intthreshold vcthresholdtype intprecision } } }",
  "variables": {
    "input": {
      "intclientid": 1,
      "intfundid": 1,
      "intratiomasterid": 1,
      "isactive": true,
      "vccondition": ">=",
      "intthreshold": 1.5,
      "vcthresholdtype": "Absolute",
      "intprecision": 2.0
    }
  }
}
```

**Response:**
```json
{
  "data": {
    "createRatioConfiguration": {
      "success": true,
      "message": "Ratio configuration created successfully",
      "ratioconfiguration": {
        "intratioconfigurationid": 1,
        "intclientid": 1,
        "intfundid": 1,
        "intratiomasterid": 1,
        "isactive": true,
        "vccondition": ">=",
        "intthreshold": 1.5,
        "vcthresholdtype": "Absolute",
        "intprecision": 2.0
      }
    }
  }
}
```

---

### 37. Update Validation Configuration

```json
{
  "query": "mutation UpdateValidationConfiguration($input: UpdateValidationConfigurationInput!) { updateValidationConfiguration(input: $input) { success message validationconfiguration { intvalidationconfigurationid intclientid intfundid intvalidationmasterid isactive vccondition intthreshold vcthresholdtype intprecision intupdatedby dtupdatedat } } }",
  "variables": {
    "input": {
      "intvalidationconfigurationid": 1,
      "isactive": false,
      "intthreshold": 150.0,
      "vccondition": ">="
      "vcthresholdtype": "Percentage"
    }
  }
}
```

**Response:**
```json
{
  "data": {
    "updateValidationConfiguration": {
      "success": true,
      "message": "Validation configuration updated successfully",
      "validationconfiguration": {
        "intvalidationconfigurationid": 1,
        "intclientid": 1,
        "intfundid": 1,
        "intvalidationmasterid": 1,
        "isactive": false,
        "vccondition": ">=",
        "intthreshold": 150.0,
        "vcthresholdtype": "Percentage",
        "intprecision": 2.0,
        "intupdatedby": 1,
        "dtupdatedat": "2025-01-28T11:00:00"
      }
    }
  }
}
```

---

### 38. Update Ratio Configuration

```json
{
  "query": "mutation UpdateRatioConfiguration($input: UpdateRatioConfigurationInput!) { updateRatioConfiguration(input: $input) { success message ratioconfiguration { intratioconfigurationid intclientid intfundid intratiomasterid isactive vccondition intthreshold vcthresholdtype intprecision intupdatedby dtupdatedat } } }",
  "variables": {
    "input": {
      "intratioconfigurationid": 1,
      "isactive": false,
      "intthreshold": 20.0,
      "vccondition": "<=",
      "vcthresholdtype": "Percentage"
    }
  }
}
```

**Response:**
```json
{
  "data": {
    "updateRatioConfiguration": {
      "success": true,
      "message": "Ratio configuration updated successfully",
      "ratioconfiguration": {
        "intratioconfigurationid": 1,
        "intclientid": 1,
        "intfundid": 1,
        "intratiomasterid": 1,
        "isactive": false,
        "vccondition": ">=",
        "intthreshold": 2.0,
        "vcthresholdtype": "Percentage",
        "intprecision": 2.0,
        "intupdatedby": 1,
        "dtupdatedat": "2025-01-28T11:00:00"
      }
    }
  }
}
```

---

### 39. Delete Validation Configuration

```json
{
  "query": "mutation DeleteValidationConfiguration($intvalidationconfigurationid: Int!) { deleteValidationConfiguration(intvalidationconfigurationid: $intvalidationconfigurationid) { success message } }",
  "variables": {
    "intvalidationconfigurationid": 1
  }
}
```

**Response:**
```json
{
  "data": {
    "deleteValidationConfiguration": {
      "success": true,
      "message": "Validation configuration 1 deleted successfully"
    }
  }
}
```

---

### 40. Delete Ratio Configuration

```json
{
  "query": "mutation DeleteRatioConfiguration($intratioconfigurationid: Int!) { deleteRatioConfiguration(intratioconfigurationid: $intratioconfigurationid) { success message } }",
  "variables": {
    "intratioconfigurationid": 1
  }
}
```

**Response:**
```json
{
  "data": {
    "deleteRatioConfiguration": {
      "success": true,
      "message": "Ratio configuration 1 deleted successfully"
    }
  }
}
```

---

## Configuration Testing Tips

### 1. Configuration Table Usage
The configuration tables (`tbl_validation_configuration` and `tbl_ratio_configuration`) allow you to:
- **Override master settings**: Per-client or per-fund specific thresholds and conditions
- **Enable/disable validations**: Control which validations/ratios are active for specific clients/funds
- **Customize thresholds**: Set different threshold values than the master defaults

### 2. Filtering Options
You can filter configurations by:
- `intvalidationmasterid` / `intratiomasterid`: Get all configurations for a specific master
- `intclientid`: Get all configurations for a specific client
- `intfundid`: Get all configurations for a specific fund
- Combinations: Filter by both client and fund for fund-specific configurations

### 3. Configuration Priority
- When both `intclientid` and `intfundid` are set, it represents a fund-specific configuration
- When only `intclientid` is set, it represents a client-wide configuration
- When both are null, it represents a global configuration (rare)

### 4. Required Fields
- `intvalidationmasterid` / `intratiomasterid`: Always required (must reference an existing master)
- `intclientid` and `intfundid`: Optional (can be null)
- `isactive`: Defaults to `false` if not specified

---

## Data Model Testing Tips

### 1. Testing Sequence for Data Models
Recommended order:
1. Test queries (23-24) to get data models and columns for validation/ratio forms
2. Test queries (25-28) to see existing data models with full details
3. Create data models (29-30) to define table structures
4. Create physical tables (31-32) to generate actual database tables
5. Verify tables exist in your database
6. Update data models (33-35) to modify existing models and columns
7. Delete test models (36) to clean up

### 2. Data Type Mapping
The system maps data types as follows:
- `Text` → `varchar(length)`
- `Date` → `date`
- `Numeric(Integer)` → `int`
- `Numeric(Decimal)` → `numeric(precision,scale)`

### 3. Required vs Optional Fields
- `ismandatory: "1"` → NOT NULL constraint
- `ismandatory: "0"` → NULL allowed
- `vcdefaultvalue` → DEFAULT value in SQL

### 4. Table Naming Convention
- Schema: Usually `validus`
- Table: `tbl_` prefix (e.g., `tbl_customer`)
- Columns: snake_case (e.g., `customer_name`)

### 5. Admin Role Required
- `createPhysicalTable` requires admin role
- `deleteDataModel` requires admin role
- Other operations require authentication only

---

## Common Issues

### 1. Authentication Error
**Error**: `Authentication required`
**Solution**: Make sure you include the Bearer token in the Authorization header

### 2. Invalid Query Syntax
**Error**: `Syntax Error`
**Solution**: Verify the query is properly formatted as a JSON string

### 3. Missing Variables
**Error**: `Variable "$intsubproductid" is not defined`
**Solution**: Make sure you include the variables object when using parameterized queries

### 4. Invalid Data Type
**Error**: `Expected type Int, found String`
**Solution**: Check that variable types match the schema (Int, String, Boolean, Float)

### 41. Get Active Validations With Configurations

Get all active validations from validation master table filtered by subproduct and optional source type, with configuration fields if configuration exists for the specified client/fund. Returns all validations in a flat structure - config fields are null if no configuration exists.

```json
{
  "query": "query GetActiveValidationsWithConfigurations($intsubproductid: Int!, $vcsourcetype: String, $intclientid: Int, $intfundid: Int) { getActiveValidationsWithConfigurations(intsubproductid: $intsubproductid, vcsourcetype: $vcsourcetype, intclientid: $intclientid, intfundid: $intfundid) { intvalidationmasterid intsubproductid vcsourcetype vctype vcsubtype issubtypeSubtotal vcvalidationname isvalidationSubtotal vcdescription intthreshold vcthresholdtype intprecision isactive intcreatedby dtcreatedat intupdatedby dtupdatedat configIsactive vccondition configThreshold configThresholdtype } }",
  "variables": {
    "intsubproductid": 1,
    "vcsourcetype": "Single",
    "intclientid": 1,
    "intfundid": 1
  }
}
```

**Response:**
```json
{
  "data": {
    "getActiveValidationsWithConfigurations": [
      {
        "intvalidationmasterid": 1,
        "intsubproductid": 1,
        "vcsourcetype": "Single",
        "vctype": "P&L",
        "vcsubtype": "NAV",
        "issubtypeSubtotal": false,
        "vcvalidationname": "Position Count Validation",
        "isvalidationSubtotal": false,
        "vcdescription": "Ensure positions count above threshold",
        "intthreshold": 100.0,
        "vcthresholdtype": "Absolute",
        "intprecision": 2.0,
        "isactive": true,
        "intcreatedby": 1,
        "dtcreatedat": "2025-01-28T10:00:00",
        "intupdatedby": null,
        "dtupdatedat": null,
        "configIsactive": true,
        "vccondition": ">=",
        "configThreshold": 100.0,
        "configThresholdtype": "Absolute"
      },
      {
        "intvalidationmasterid": 2,
        "intsubproductid": 1,
        "vcsourcetype": "Single",
        "vctype": "P&L",
        "vcsubtype": "Market Value",
        "issubtypeSubtotal": false,
        "vcvalidationname": "Market Value Validation",
        "isvalidationSubtotal": false,
        "vcdescription": "Validates market value calculations",
        "intthreshold": 0.05,
        "vcthresholdtype": "Percentage",
        "intprecision": 4.0,
        "isactive": true,
        "intcreatedby": 1,
        "dtcreatedat": "2025-01-28T10:00:00",
        "intupdatedby": null,
        "dtupdatedat": null,
        "configIsactive": null,
        "vccondition": null,
        "configThreshold": null,
        "configThresholdtype": null
      }
    ]
  }
}
```

**Note:** 
- Returns all active validations (where `validationmaster.isactive = true`) filtered by `intsubproductid` and optional `vcsourcetype`
- If a configuration exists for the specified `intclientid` and `intfundid`, the config fields (`configIsactive`, `vccondition`, `configThreshold`, `configThresholdtype`) will have values
- If no configuration exists, these 4 fields will be `null`
- The response is a flat structure with all validation master fields and config fields at the same level

### 42. Bulk Upsert Validation Configurations

```json
{
  "query": "mutation UpsertValidationConfigurationsBulk($input: BulkValidationConfigurationUpsertInput!) { upsertValidationConfigurationsBulk(input: $input) { success message createdCount updatedCount skippedCount configurations { intvalidationconfigurationid intclientid intfundid intvalidationmasterid isactive vccondition intthreshold vcthresholdtype intprecision intcreatedby dtcreatedat intupdatedby dtupdatedat vcvalidationname subproduct { intsubproductid vcsubproductname } client { id name } fund { id name } } } }",
  "variables": {
    "input": {
      "items": [
        {
          "intvalidationmasterid": 1,
          "vcsourcetype": "Single",
          "intclientid": 1,
          "intfundid": 1,
          "isactive": true,
          "vccondition": ">=",
          "intthreshold": 100.0,
          "vcthresholdtype": "Absolute",
          "intprecision": 2.0
        },
        {
          "intvalidationmasterid": 2,
          "vcsourcetype": "Dual",
          "intclientid": 1,
          "intfundid": 1,
          "isactive": false,
          "vccondition": "<=",
          "intthreshold": 10.0,
          "vcthresholdtype": "Percentage"
        },
        {
          "intvalidationmasterid": 3,
          "intclientid": 1,
          "intfundid": 2,
          "isactive": true,
          "vccondition": "=",
          "intthreshold": 0.0,
          "vcthresholdtype": "Absolute"
        }
      ]
    }
  }
}
```

**Response:**
```json
{
  "data": {
    "upsertValidationConfigurationsBulk": {
      "success": true,
      "message": "Bulk upsert completed",
      "createdCount": 2,
      "updatedCount": 1,
      "skippedCount": 1,
      "configurations": [
        {
          "intvalidationconfigurationid": 10,
          "intclientid": 1,
          "intfundid": 1,
          "intvalidationmasterid": 1,
          "isactive": true,
          "vccondition": ">=",
          "intthreshold": 100.0,
          "vcthresholdtype": "Absolute",
          "intprecision": 2.0,
          "intcreatedby": 1,
          "dtcreatedat": "2025-10-30T10:00:00",
          "intupdatedby": null,
          "dtupdatedat": null,
          "vcvalidationname": "Position Count Validation",
          "subproduct": { "intsubproductid": 1, "vcsubproductname": "NAV" },
          "client": { "id": 1, "name": "Example Client" },
          "fund": { "id": 1, "name": "Example Fund" }
        }
      ]
    }
  }
}
```

### 43. Get Active Ratios With Configurations

Get all active ratios from ratio master table filtered by subproduct and optional source type, with configuration fields if configuration exists for the specified client/fund. Returns all ratios in a flat structure - config fields are null if no configuration exists.

```json
{
  "query": "query GetActiveRatiosWithConfigurations($intsubproductid: Int!, $vcsourcetype: String, $intclientid: Int, $intfundid: Int) { getActiveRatiosWithConfigurations(intsubproductid: $intsubproductid, vcsourcetype: $vcsourcetype, intclientid: $intclientid, intfundid: $intfundid) { intratiomasterid intsubproductid vcsourcetype vctype vcrationame isratioSubtotal vcdescription intthreshold vcthresholdtype intprecision isactive intcreatedby dtcreatedat intupdatedby dtupdatedat configIsactive vccondition configThreshold configThresholdtype } }",
  "variables": {
    "intsubproductid": 1,
    "vcsourcetype": "Single",
    "intclientid": 1,
    "intfundid": 1
  }
}
```

**Response:**
```json
{
  "data": {
    "getActiveRatiosWithConfigurations": [
      {
        "intratiomasterid": 1,
        "intsubproductid": 1,
        "vcsourcetype": "Single",
        "vctype": "Financial",
        "vcrationame": "Debt to Equity Ratio",
        "isratioSubtotal": false,
        "vcdescription": "Calculates debt to equity ratio",
        "intthreshold": 2.0,
        "vcthresholdtype": "Absolute",
        "intprecision": 4.0,
        "isactive": true,
        "intcreatedby": 1,
        "dtcreatedat": "2025-01-28T10:00:00",
        "intupdatedby": null,
        "dtupdatedat": null,
        "configIsactive": true,
        "vccondition": ">=",
        "configThreshold": 1.5,
        "configThresholdtype": "Absolute"
      },
      {
        "intratiomasterid": 2,
        "intsubproductid": 1,
        "vcsourcetype": "Single",
        "vctype": "Liquidity",
        "vcrationame": "Current Ratio",
        "isratioSubtotal": false,
        "vcdescription": "Measures ability to pay short-term obligations",
        "intthreshold": 1.5,
        "vcthresholdtype": "Absolute",
        "intprecision": 4.0,
        "isactive": true,
        "intcreatedby": 1,
        "dtcreatedat": "2025-01-28T10:00:00",
        "intupdatedby": null,
        "dtupdatedat": null,
        "configIsactive": null,
        "vccondition": null,
        "configThreshold": null,
        "configThresholdtype": null
      }
    ]
  }
}
```

**Note:** 
- Returns all active ratios (where `ratiomaster.isactive = true`) filtered by `intsubproductid` and optional `vcsourcetype`
- If a configuration exists for the specified `intclientid` and `intfundid`, the config fields (`configIsactive`, `vccondition`, `configThreshold`, `configThresholdtype`) will have values
- If no configuration exists, these 4 fields will be `null`
- The response is a flat structure with all ratio master fields and config fields at the same level

### 44. Bulk Upsert Ratio Configurations

Create or update ratio configurations in bulk. Only processes items where `isactive` is `true`; inactive items are skipped. If a configuration exists for the same `intclientid`, `intfundid`, and `intratiomasterid`, it will be updated. Otherwise, a new configuration will be created.

```json
{
  "query": "mutation UpsertRatioConfigurationsBulk($input: BulkRatioConfigurationUpsertInput!) { upsertRatioConfigurationsBulk(input: $input) { success message createdCount updatedCount skippedCount configurations { intratioconfigurationid intclientid intfundid intratiomasterid isactive vccondition intthreshold vcthresholdtype intprecision intcreatedby dtcreatedat intupdatedby dtupdatedat vcrationame subproduct { intsubproductid vcsubproductname } client { id name } fund { id name } } } }",
  "variables": {
    "input": {
      "items": [
        {
          "intratiomasterid": 1,
          "vcsourcetype": "Single",
          "intclientid": 1,
          "intfundid": 1,
          "isactive": true,
          "vccondition": ">=",
          "intthreshold": 1.5,
          "vcthresholdtype": "Absolute",
          "intprecision": 2.0
        },
        {
          "intratiomasterid": 2,
          "vcsourcetype": "Single",
          "intclientid": 1,
          "intfundid": 1,
          "isactive": false,
          "vccondition": "<=",
          "intthreshold": 10.0,
          "vcthresholdtype": "Percentage"
        },
        {
          "intratiomasterid": 3,
          "intclientid": 1,
          "intfundid": 2,
          "isactive": true,
          "vccondition": "=",
          "intthreshold": 0.0,
          "vcthresholdtype": "Absolute"
        }
      ]
    }
  }
}
```

**Response:**
```json
{
  "data": {
    "upsertRatioConfigurationsBulk": {
      "success": true,
      "message": "Bulk upsert completed",
      "createdCount": 2,
      "updatedCount": 1,
      "skippedCount": 1,
      "configurations": [
        {
          "intratioconfigurationid": 10,
          "intclientid": 1,
          "intfundid": 1,
          "intratiomasterid": 1,
          "isactive": true,
          "vccondition": ">=",
          "intthreshold": 1.5,
          "vcthresholdtype": "Absolute",
          "intprecision": 2.0,
          "intcreatedby": 1,
          "dtcreatedat": "2025-10-30T10:00:00",
          "intupdatedby": null,
          "dtupdatedat": null,
          "vcrationame": "Debt to Equity Ratio",
          "subproduct": { "intsubproductid": 1, "vcsubproductname": "NAV" },
          "client": { "id": 1, "name": "Example Client" },
          "fund": { "id": 1, "name": "Example Fund" }
        }
      ]
    }
  }
}
```

---

### 45. Get Fund Compare Validation Summary

Get validation and ratio summary for a single fund compare operation. Returns counts for the latest process instances matching the specified criteria.

**Query:**
```json
  {
    "query": "query GetFundCompareValidationSummary($clientId: Int!, $fundId: Int!, $subproductId: Int!, $sourceA: String!, $dateA: String!, $sourceB: String, $dateB: String) { getFundCompareValidationSummary(clientId: $clientId, fundId: $fundId, subproductId: $subproductId, sourceA: $sourceA, dateA: $dateA, sourceB: $sourceB, dateB: $dateB) { validationTotal validationFailed validationPassed validationExceptions ratioTotal ratioFailed ratioPassed validationProcessInstanceId ratioProcessInstanceId } }",
    "variables": {
      "clientId": 1,
      "fundId": 1,
      "subproductId": 1,
      "sourceA": "",
      "dateA": "2024-01-31",
      "dateB": "2024-02-29"
    }
  }
```

**Response:**
```json
{
  "data": {
    "getFundCompareValidationSummary": {
      "validationTotal": 15,
      "validationFailed": 3,
      "validationPassed": 12,
      "validationExceptions": 5,
      "ratioTotal": 8,
      "ratioFailed": 1,
      "ratioPassed": 7,
      "validationProcessInstanceId": 123,
      "ratioProcessInstanceId": 124
    }
  }
}
```

**Note:**
- Returns `null` if no process instances match the criteria
- `sourceB` and `dateB` are optional (for single-source validations, omit them or set to the same as `sourceA` and `dateA`)
- `validationTotal` is the count of unique `intvalidationconfigurationid` for the process instance
- `validationFailed` is the count of unique `intvalidationconfigurationid` with at least one failed status
- `validationPassed` is `validationTotal - validationFailed`
- `validationExceptions` is the total number of failed validation rows for that process instance
- Similar logic applies to ratio counts

---

### 46. Get Validation Aggregated Data

Get validation aggregated data from validation results for a specific process instance. Returns detailed information about each validation including status and exception counts.

**Note:** You can call this API in two ways:
1. **With `processInstanceId`**: Directly specify the process instance ID
2. **Without `processInstanceId`**: Provide `fundId`, `subproductId`, `sourceA`, and `dateA` to automatically find the latest process instance

#### 46a. Get Validation Aggregated Data (With Process Instance ID)

**Query:**
```json
{
  "query": "query GetValidationAggregatedData($clientId: Int!, $processInstanceId: Int) { getValidationAggregatedData(clientId: $clientId, processInstanceId: $processInstanceId) { vcvalidationname type subtype configThreshold status exception } }",
  "variables": {
    "clientId": 1,
    "processInstanceId": 123
  }
}
```

**Response:**
```json
{
  "data": {
    "getValidationAggregatedData": [
      {
        "vcvalidationname": "Position Count Validation",
        "type": "P&L",
        "subtype": "NAV",
        "configThreshold": 100.0,
        "status": "Failed",
        "exception": 3
      },
      {
        "vcvalidationname": "Market Value Validation",
        "type": "P&L",
        "subtype": "Market Value",
        "configThreshold": 0.05,
        "status": "Passed",
        "exception": 0
      },
      {
        "vcvalidationname": "Price Comparison Validation",
        "type": "P&L",
        "subtype": "Pricing",
        "configThreshold": 0.01,
        "status": "Failed",
        "exception": 2
      }
    ]
  }
}
```

#### 46b. Get Validation Aggregated Data (Without Process Instance ID)

When `processInstanceId` is not provided, the API will automatically find the latest validation process instance using the other parameters.

**Query:**
```json
{
  "query": "query GetValidationAggregatedData($clientId: Int!, $fundId: Int, $subproductId: Int, $sourceA: String, $dateA: String, $sourceB: String, $dateB: String) { getValidationAggregatedData(clientId: $clientId, fundId: $fundId, subproductId: $subproductId, sourceA: $sourceA, dateA: $dateA, sourceB: $sourceB, dateB: $dateB) { vcvalidationname type subtype configThreshold status exception } }",
  "variables": {
    "clientId": 1,
    "fundId": 1,
    "subproductId": 1,
    "sourceA": "Bluefield",
    "dateA": "2025-01-15",
    "sourceB": "Bloomberg",
    "dateB": "2025-01-15"
  }
}
```

**Response:**
```json
{
  "data": {
    "getValidationAggregatedData": [
      {
        "vcvalidationname": "Position Count Validation",
        "type": "P&L",
        "subtype": "NAV",
        "configThreshold": 100.0,
        "status": "Failed",
        "exception": 3
      },
      {
        "vcvalidationname": "Market Value Validation",
        "type": "P&L",
        "subtype": "Market Value",
        "configThreshold": 0.05,
        "status": "Passed",
        "exception": 0
      }
    ]
  }
}
```

**Note:**
- Either `processInstanceId` must be provided, OR (`fundId`, `subproductId`, `sourceA`, `dateA`) must all be provided
- `sourceB` and `dateB` are optional (for single-source validations, omit them or set to the same as `sourceA` and `dateA`)
- Returns a list of all validations for the specified or found `process_instance_id`
- `vcvalidationname` comes from `ValidationMaster`
- `type` and `subtype` come from `ValidationMaster`
- `configThreshold` comes from `ValidationConfiguration`
- `status` is "Failed" if any validation result has a failed status for that `intprocessinstanceid` and `intvalidationconfigurationid`, otherwise "Passed"
- `exception` is the count of failed validation rows for that `intprocessinstanceid` and `intvalidationconfigurationid`
- If no validation results exist for a configuration, it will not appear in the results
- If no process instance is found when using the alternative parameters, returns an empty array

---

### 47. Get Validation Comparison Data

Get validation comparison data with side A and side B joined. Returns detailed validation results with matched sides, including dynamic columns based on the validation configuration.

**Note:** You can call this API in two ways:
1. **With `processInstanceId`**: Directly specify the process instance ID
2. **Without `processInstanceId`**: Provide `fundId`, `subproductId`, and `dateA` to automatically find the latest process instance

#### 47a. Get Validation Comparison Data (With Process Instance ID)

**Query:**
```json
{
  "query": "query GetValidationComparisonData($clientId: Int!, $processInstanceId: Int) { getValidationComparisonData(clientId: $clientId, processInstanceId: $processInstanceId) { intprocessinstanceid validations intmatchid data } }",
  "variables": {
    "clientId": 1,
    "processInstanceId": 123
  }
}
```

**Response:**
```json
{
  "data": {
    "getValidationComparisonData": [
      {
        "intprocessinstanceid": 123,
        "validations": "Position Count Validation",
        "intmatchid": 1,
        "data": {
          "investmentdescription": "Apple Inc.",
          "assettype": "Equity",
          "Source_A_value": 150.25,
          "Source_B_value": 150.30,
          "status": "Passed",
          "intformulaoutput": 0.05,
          "tooltip": 0.0333
        }
      },
      {
        "intprocessinstanceid": 123,
        "validations": "Position Count Validation",
        "intmatchid": 2,
        "data": {
          "investmentdescription": "Microsoft Corp.",
          "assettype": "Equity",
          "Source_A_value": 350.50,
          "Source_B_value": 350.45,
          "status": "Failed",
          "intformulaoutput": -0.05,
          "tooltip": 0.0143
        }
      }
    ]
  }
}
```

#### 47b. Get Validation Comparison Data (Without Process Instance ID)

When `processInstanceId` is not provided, the API will automatically find the latest validation process instance using the other parameters.

**Query:**
```json
{
  "query": "query GetValidationComparisonData($clientId: Int!, $fundId: Int, $subproductId: Int, $dateA: String, $sourceA: String, $sourceB: String, $dateB: String) { getValidationComparisonData(clientId: $clientId, fundId: $fundId, subproductId: $subproductId, dateA: $dateA, sourceA: $sourceA, sourceB: $sourceB, dateB: $dateB) { intprocessinstanceid validations intmatchid data } }",
  "variables": {
    "clientId": 2,
    "fundId": 1,
    "subproductId": 1,
    "sourceA": "",
    "dateA": "2024-01-31",
    "dateB": "2024-02-29"
  }
}
```

**Response:**
```json
{
  "data": {
    "getValidationComparisonData": [
      {
        "intprocessinstanceid": 123,
        "validations": "Market Value Validation",
        "intmatchid": 1,
        "data": {
          "investmentdescription": "Apple Inc.",
          "assettype": "Equity",
          "Source_A_value": 150.25,
          "Source_B_value": 150.30,
          "status": "Passed",
          "intformulaoutput": 0.05,
          "tooltip": 0.0333
        }
      }
    ]
  }
}
```

**Note:**
- Either `processInstanceId` must be provided, OR (`fundId`, `subproductId`, `dateA`) must all be provided
- `sourceA`, `sourceB`, and `dateB` are optional (for single-source validations, omit them or set to the same as `sourceA` and `dateA`)
- The `data` field contains dynamic columns that vary based on the validation configuration:
  - **Description column**: Determined from `intgroup_attributeid` in `tbl_validation_details` (first align key is used), coalesced from side A and B
  - **Assettype column**: Determined from `intassettypeid` in `tbl_validation_details` (second align key if present), coalesced from side A and B
  - **Formula column**: Extracted from `vcformula` in `tbl_validation_details` using regex pattern `\[([^\[\]]+)\](?!.*\[)`
  - The formula column appears as `Source_A_value` and `Source_B_value` for side A and side B respectively
  - **Status**: Coalesced from side A and side B (uses side A if available, otherwise side B)
  - **intformulaoutput**: Formula output value from the validation result
  - **Tooltip**: Calculated based on `threshold_type`:
    - If `threshold_type` is `%` or `Percentage`: Returns signed difference `(Source_B_value - Source_A_value)`
    - Otherwise: Returns percentage difference `ABS((Source_B_value - Source_A_value)) * 100 / Source_A_value`
- The data model table to join is determined from `intdatamodelid` in `tbl_validation_details`
- Only results with `vcside = 'A'` are returned, joined with matching `vcside = 'B'` results on `intmatchid`
- Results are ordered by `intmatchid`
- If no process instance is found when using the alternative parameters, returns an empty array

---

### 48. Get Ratio Comparison Data

Get ratio comparison data with side A and side B joined. Returns detailed ratio results with matched sides, including all ratio configuration, master, details, and result fields.

**Note:** You can call this API in two ways:
1. **With `processInstanceId`**: Directly specify the process instance ID
2. **Without `processInstanceId`**: Provide `fundId`, `subproductId`, and `dateA` to automatically find the latest process instance

#### 48a. Get Ratio Comparison Data (With Process Instance ID)

**Query:**
```json
{
  "query": "query GetRatioComparisonData($clientId: Int!, $processInstanceId: Int) { getRatioComparisonData(clientId: $clientId, processInstanceId: $processInstanceId) { intprocessinstanceid ratios intmatchid data } }",
  "variables": {
    "clientId": 1,
    "processInstanceId": 125
  }
}
```

**Response:**
```json
{
  "data": {
    "getRatioComparisonData": [
      {
        "intprocessinstanceid": 125,
        "ratios": "Debt to Equity Ratio",
        "intmatchid": 1,
        "data": {
          "align_key": "Investment A",
          "assettype": "Equity",
          "intratioconfigurationid": 1,
          "intratiomasterid": 1,
          "intthreshold": 2.0,
          "vcthresholdtype": "Absolute",
          "intprecision": 4.0,
          "vcrationame": "Debt to Equity Ratio",
          "vctype": "Financial",
          "vcdescription": "Calculates debt to equity ratio",
          "vcformula": "((Period2.`Numerator`/Period2.`Denominator` - Period1.`Numerator`/Period1.`Denominator`)*100)/(Period1.`Numerator`/Period1.`Denominator`)",
          "vcnumerator": "Displayname: Total Debt\n(SUM(CASE WHEN {'Trial Balance'}.[Account Type] = 'Liabilities' THEN ABS({'Trial Balance'}.[Closing Balance]) ELSE 0 END))",
          "vcdenominator": "Displayname: Total Equity\n(SUM(CASE WHEN {'Trial Balance'}.[Account Type] = 'Equity' THEN {'Trial Balance'}.[Closing Balance] ELSE 0 END))",
          "intnumeratoroutput_a": 5000000.0,
          "intnumeratoroutput_b": 5100000.0,
          "intdenominatoroutput_a": 10000000.0,
          "intdenominatoroutput_b": 10000000.0,
          "source_a_value": 0.5,
          "source_b_value": 0.51,
          "vcformulaoutput_a": "0.50",
          "vcformulaoutput_b": "0.51",
          "vcstatus": "Passed",
          "tooltipinfo": 0.01
        }
      },
      {
        "intprocessinstanceid": 125,
        "ratios": "Debt to Equity Ratio",
        "intmatchid": 2,
        "data": {
          "align_key": "Investment B",
          "assettype": "Fixed Income",
          "intratioconfigurationid": 1,
          "intratiomasterid": 1,
          "intthreshold": 2.0,
          "vcthresholdtype": "Absolute",
          "intprecision": 4.0,
          "vcrationame": "Debt to Equity Ratio",
          "vctype": "Financial",
          "vcdescription": "Calculates debt to equity ratio",
          "vcformula": "((Period2.`Numerator`/Period2.`Denominator` - Period1.`Numerator`/Period1.`Denominator`)*100)/(Period1.`Numerator`/Period1.`Denominator`)",
          "vcnumerator": "Displayname: Total Debt\n(SUM(CASE WHEN {'Trial Balance'}.[Account Type] = 'Liabilities' THEN ABS({'Trial Balance'}.[Closing Balance]) ELSE 0 END))",
          "vcdenominator": "Displayname: Total Equity\n(SUM(CASE WHEN {'Trial Balance'}.[Account Type] = 'Equity' THEN {'Trial Balance'}.[Closing Balance] ELSE 0 END))",
          "intnumeratoroutput_a": 3000000.0,
          "intnumeratoroutput_b": 3200000.0,
          "intdenominatoroutput_a": 8000000.0,
          "intdenominatoroutput_b": 8000000.0,
          "source_a_value": 0.375,
          "source_b_value": 0.4,
          "vcformulaoutput_a": "0.375",
          "vcformulaoutput_b": "0.40",
          "vcstatus": "Failed",
          "tooltipinfo": 0.025
        }
      }
    ]
  }
}
```

#### 48b. Get Ratio Comparison Data (Without Process Instance ID)

When `processInstanceId` is not provided, the API will automatically find the latest ratio process instance using the other parameters.

**Query:**
```json
{
  "query": "query GetRatioComparisonData($clientId: Int!, $fundId: Int, $subproductId: Int, $dateA: String, $sourceA: String, $sourceB: String, $dateB: String) { getRatioComparisonData(clientId: $clientId, fundId: $fundId, subproductId: $subproductId, dateA: $dateA, sourceA: $sourceA, sourceB: $sourceB, dateB: $dateB) { intprocessinstanceid ratios intmatchid data } }",
  "variables": {
    "clientId": 2,
    "fundId": 1,
    "subproductId": 1,
    "sourceA": "Bluefield",
    "dateA": "2024-01-31",
    "dateB": "2024-02-29"
  }
}
```

**Response:**
```json
{
  "data": {
    "getRatioComparisonData": [
      {
        "intprocessinstanceid": 125,
        "ratios": "Current Ratio",
        "intmatchid": 1,
        "data": {
          "align_key": "Investment A",
          "assettype": "Equity",
          "intratioconfigurationid": 2,
          "intratiomasterid": 2,
          "intthreshold": 1.5,
          "vcthresholdtype": "Absolute",
          "intprecision": 4.0,
          "vcrationame": "Current Ratio",
          "vctype": "Liquidity",
          "vcdescription": "Measures ability to pay short-term obligations",
          "vcformula": "((Period2.`Numerator`/Period2.`Denominator` - Period1.`Numerator`/Period1.`Denominator`)*100)/(Period1.`Numerator`/Period1.`Denominator`)",
          "vcnumerator": "Displayname: Current Assets\n(SUM(CASE WHEN {'Trial Balance'}.[Account Type] = 'Assets' AND {'Trial Balance'}.[Sub Type] = 'Current' THEN {'Trial Balance'}.[Closing Balance] ELSE 0 END))",
          "vcdenominator": "Displayname: Current Liabilities\n(SUM(CASE WHEN {'Trial Balance'}.[Account Type] = 'Liabilities' AND {'Trial Balance'}.[Sub Type] = 'Current' THEN ABS({'Trial Balance'}.[Closing Balance]) ELSE 0 END))",
          "intnumeratoroutput_a": 5000000.0,
          "intnumeratoroutput_b": 5200000.0,
          "intdenominatoroutput_a": 2000000.0,
          "intdenominatoroutput_b": 2000000.0,
          "source_a_value": 2.5,
          "source_b_value": 2.6,
          "vcformulaoutput_a": "2.50",
          "vcformulaoutput_b": "2.60",
          "vcstatus": "Passed",
          "tooltipinfo": 0.1
        }
      }
    ]
  }
}
```

**Note:**
- Either `processInstanceId` must be provided, OR (`fundId`, `subproductId`, `dateA`) must all be provided
- `sourceA`, `sourceB`, and `dateB` are optional (for single-source ratios, omit them or set to the same as `sourceA` and `dateA`)
- The `data` field contains all ratio-related fields:
  - **From `tbl_ratio_configuration`**: `intratiomasterid`, `intthreshold`, `vcthresholdtype`, `intprecision`
  - **From `tbl_ratio_master`**: `vcrationame`, `vctype`, `vcdescription`
  - **From `tbl_ratio_details`**: `vcformula`, `vcnumerator`, `vcdenominator`
  - **From `tbl_ratio_result` (combined by `intmatchid`)**:
    - `intnumeratoroutput_a` and `intnumeratoroutput_b`: Numerator values for side A and B
    - `intdenominatoroutput_a` and `intdenominatoroutput_b`: Denominator values for side A and B
    - `source_a_value` and `source_b_value`: Formula output values (`intformulaoutput`) for side A and B
    - `vcformulaoutput_a` and `vcformulaoutput_b`: Formula output text for side A and B
    - `vcstatus`: Coalesced from side A and side B (uses side A if available, otherwise side B)
  - **Dynamic align keys**: `align_key` and `assettype` (if available in ratio details) - coalesced from side A and B
  - **Tooltip**: Calculated based on `vcthresholdtype`:
    - If `vcthresholdtype` contains `%` or `Percentage`: Returns signed difference `(source_b_value - source_a_value)`
    - Otherwise: Returns percentage difference `ABS((source_b_value - source_a_value)) * 100 / source_a_value`
- The data model table to join is determined from `intdatamodelid` in `tbl_ratio_details`
- Only results with `vcside = 'A'` are returned, joined with matching `vcside = 'B'` results on `intmatchid`
- Results are ordered by `intmatchid`
- If no process instance is found when using the alternative parameters, returns an empty array

---

### 49. Get Report Ingested

Get the list of ingested reports for a particular client, fund, source(s), and date(s). Handles both dual-source (2 sources, 1 date) and single-source (1 source, 2 dates) scenarios.

**Query:**
```json
{
  "query": "query GetReportIngested($clientId: Int!, $fundId: Int!, $sourceA: String, $sourceB: String, $dateA: String, $dateB: String) { getReportIngested(clientId: $clientId, fundId: $fundId, sourceA: $sourceA, sourceB: $sourceB, dateA: $dateA, dateB: $dateB) { dataSection { groupKey files { category fileName fileFormat source time status } } versionSection { groupKey categories { category files { fileName fileFormat source time status version } } } } }",
  "variables": {
    "clientId": 2,
    "fundId": 1,
    "sourceA": "Bluefield",
    "sourceB": "Bloomberg",
    "dateA": "2024-01-31"
  }
}
```

**Response (Dual Source - 2 sources, 1 date):**
```json
{
  "data": {
    "getReportIngested": {
      "dataSection": [
        {
          "groupKey": "Bluefield",
          "files": [
            {
              "category": "Portfolio Valuation By Instrument",
              "fileName": "portfolio_valuation_20240131.xlsx",
              "fileFormat": "xlsx",
              "source": "File Upload",
              "time": "2024-01-31T10:30:00",
              "status": "Completed"
            },
            {
              "category": "Trial Balance",
              "fileName": "trial_balance_20240131.csv",
              "fileFormat": "csv",
              "source": "File Upload",
              "time": "2024-01-31T11:15:00",
              "status": "Completed"
            }
          ]
        },
        {
          "groupKey": "Bloomberg",
          "files": [
            {
              "category": "Portfolio Valuation By Instrument",
              "fileName": "bloomberg_portfolio_20240131.xlsx",
              "fileFormat": "xlsx",
              "source": "API",
              "time": "2024-01-31T12:00:00",
              "status": "Completed"
            }
          ]
        }
      ],
      "versionSection": [
        {
          "groupKey": "Bluefield",
          "categories": [
            {
              "category": "Portfolio Valuation By Instrument",
              "files": [
                {
                  "fileName": "portfolio_valuation_20240131_v1.xlsx",
                  "fileFormat": "xlsx",
                  "source": "File Upload",
                  "time": "2024-01-31T10:30:00",
                  "status": "Completed",
                  "version": "1"
                },
                {
                  "fileName": "portfolio_valuation_20240131_v2.xlsx",
                  "fileFormat": "xlsx",
                  "source": "File Upload",
                  "time": "2024-01-31T14:20:00",
                  "status": "Completed",
                  "version": "2"
                }
              ]
            },
            {
              "category": "Trial Balance",
              "files": [
                {
                  "fileName": "trial_balance_20240131.csv",
                  "fileFormat": "csv",
                  "source": "File Upload",
                  "time": "2024-01-31T11:15:00",
                  "status": "Completed",
                  "version": "1"
                }
              ]
            }
          ]
        },
        {
          "groupKey": "Bloomberg",
          "categories": [
            {
              "category": "Portfolio Valuation By Instrument",
              "files": [
                {
                  "fileName": "bloomberg_portfolio_20240131.xlsx",
                  "fileFormat": "xlsx",
                  "source": "API",
                  "time": "2024-01-31T12:00:00",
                  "status": "Completed",
                  "version": "1"
                }
              ]
            }
          ]
        }
      ]
    }
  }
}
```

**Query (Single Source - 1 source, 2 dates):**
```json
{
  "query": "query GetReportIngested($clientId: Int!, $fundId: Int!, $sourceA: String, $dateA: String, $dateB: String) { getReportIngested(clientId: $clientId, fundId: $fundId, sourceA: $sourceA, dateA: $dateA, dateB: $dateB) { dataSection { groupKey files { category fileName fileFormat source time status } } versionSection { groupKey categories { category files { fileName fileFormat source time status version } } } } }",
  "variables": {
    "clientId": 2,
    "fundId": 1,
    "sourceA": "Bluefield",
    "dateA": "2024-01-31",
    "dateB": "2024-02-29"
  }
}
```

**Response (Single Source - 1 source, 2 dates):**
```json
{
  "data": {
    "getReportIngested": {
      "dataSection": [
        {
          "groupKey": "2024-01-31",
          "files": [
            {
              "category": "Portfolio Valuation By Instrument",
              "fileName": "portfolio_valuation_20240131.xlsx",
              "fileFormat": "xlsx",
              "source": "File Upload",
              "time": "2024-01-31T10:30:00",
              "status": "Completed"
            }
          ]
        },
        {
          "groupKey": "2024-02-29",
          "files": [
            {
              "category": "Portfolio Valuation By Instrument",
              "fileName": "portfolio_valuation_20240229.xlsx",
              "fileFormat": "xlsx",
              "source": "File Upload",
              "time": "2024-02-29T10:30:00",
              "status": "Completed"
            }
          ]
        }
      ],
      "versionSection": [
        {
          "groupKey": "2024-01-31",
          "categories": [
            {
              "category": "Portfolio Valuation By Instrument",
              "files": [
                {
                  "fileName": "portfolio_valuation_20240131_v1.xlsx",
                  "fileFormat": "xlsx",
                  "source": "File Upload",
                  "time": "2024-01-31T10:30:00",
                  "status": "Completed",
                  "version": "1"
                },
                {
                  "fileName": "portfolio_valuation_20240131_v2.xlsx",
                  "fileFormat": "xlsx",
                  "source": "File Upload",
                  "time": "2024-01-31T14:20:00",
                  "status": "Completed",
                  "version": "2"
                }
              ]
            }
          ]
        },
        {
          "groupKey": "2024-02-29",
          "categories": [
            {
              "category": "Portfolio Valuation By Instrument",
              "files": [
                {
                  "fileName": "portfolio_valuation_20240229.xlsx",
                  "fileFormat": "xlsx",
                  "source": "File Upload",
                  "time": "2024-02-29T10:30:00",
                  "status": "Completed",
                  "version": "1"
                }
              ]
            }
          ]
        }
      ]
    }
  }
}
```

**Note:**
- **Dual Source (2 sources, 1 date)**: Provide `sourceA`, `sourceB`, and `dateA`. `dateB` should be omitted or null.
- **Single Source (1 source, 2 dates)**: Provide `sourceA`, `dateA`, and `dateB`. `sourceB` should be omitted or null.
- **Data Section**: Groups files by source name (dual source) or date (single source). Each file includes:
  - `category`: Data model name from `DataModelMaster.vcmodelname`
  - `fileName`: From `tbl_data_load_instance.vcdataloaddescription`
  - `fileFormat`: Extracted from file name extension
  - `source`: From `tbl_data_load_instance.vcloadtype`
  - `time`: From `tbl_data_load_instance.dtloadedat` (ISO format)
  - `status`: From `tbl_data_load_instance.vcloadstatus`
- **Version Section**: Groups first by source/date, then by category (data model). Each file includes:
  - All fields from data section
  - `version`: Sequential version number (1, 2, 3...) for files with the same combination of (fund, client, source, date, datamodel), ordered by `dtloadedat`
- Returns `null` if no data is found for the specified criteria

---

### 50. Get Data Load Combinations

Get a list of unique combinations of (client, fund, source, date) from the `tbl_data_load_instance` table. Useful for populating dropdowns or filters in the UI.

**Query (Get All Combinations):**
```json
{
  "query": "query GetDataLoadCombinations { getDataLoadCombinations { clientId fundId source date } }"
}
```

**Response:**
```json
{
  "data": {
    "getDataLoadCombinations": [
      {
        "clientId": 2,
        "fundId": 1,
        "source": "Bluefield",
        "date": "2024-01-31"
      },
      {
        "clientId": 2,
        "fundId": 1,
        "source": "Bluefield",
        "date": "2024-02-29"
      },
      {
        "clientId": 2,
        "fundId": 1,
        "source": "Bloomberg",
        "date": "2024-01-31"
      },
      {
        "clientId": 2,
        "fundId": 2,
        "source": "Harborview",
        "date": "2024-01-31"
      }
    ]
  }
}
```

**Query (Filter by Client ID):**
```json
{
  "query": "query GetDataLoadCombinations($clientId: Int) { getDataLoadCombinations(clientId: $clientId) { clientId fundId source date } }",
  "variables": {
    "clientId": 2
  }
}
```

**Query (Filter by Client ID and Fund ID):**
```json
{
  "query": "query GetDataLoadCombinations($clientId: Int, $fundId: Int) { getDataLoadCombinations(clientId: $clientId, fundId: $fundId) { clientId fundId source date } }",
  "variables": {
    "clientId": 2,
    "fundId": 1
  }
}
```

**Note:**
- Returns unique combinations of (client_id, fund_id, source, date) from `tbl_data_load_instance`
- Only includes records where all four fields are not null
- Results are sorted by client_id, fund_id, source, and date
- `clientId` and `fundId` are optional filters - if provided, only combinations matching those values are returned
- `date` is returned in ISO format (YYYY-MM-DD)
- `source` is the value from `vcdatasourcename` field

---

### 51. Get Latest Process Instance Summary

Get the latest process instance summary for a given client, fund, subproduct, source(s), and date(s). Returns validation and ratio counts, process instance IDs, and detailed subchecks information.

**Query:**
```json
{
  "query": "query GetLatestProcessInstanceSummary($clientId: Int!, $fundId: Int!, $subproductId: Int!, $sourceA: String!, $dateA: String!, $sourceB: String, $dateB: String) { getFundCompareValidationSummary(clientId: $clientId, fundId: $fundId, subproductId: $subproductId, sourceA: $sourceA, dateA: $dateA, sourceB: $sourceB, dateB: $dateB) { validationTotal validationFailed validationPassed validationExceptions ratioTotal ratioFailed ratioPassed validationProcessInstanceId ratioProcessInstanceId } }",
  "variables": {
    "clientId": 2,
    "fundId": 1,
    "subproductId": 1,
    "sourceA": "Bluefield",
    "dateA": "2024-01-31",
    "sourceB": "Bloomberg",
    "dateB": null
  }
}
```

**Note:** The `subchecks` field is available in the service layer but may need to be added to the GraphQL schema (`ProcessInstanceSummaryType`) to be queryable. If added, the query would include:
```graphql
subchecks {
  subtype
  status
  validations {
    validationName
    description
    status
    passFail
    datetime
  }
}
```

**Response:**
```json
{
  "data": {
    "getFundCompareValidationSummary": {
      "validationTotal": 15,
      "validationFailed": 3,
      "validationPassed": 12,
      "validationExceptions": 5,
      "ratioTotal": 8,
      "ratioFailed": 1,
      "ratioPassed": 7,
      "validationProcessInstanceId": 123,
      "ratioProcessInstanceId": 124
    }
  }
}
```

**Response (with subchecks - if added to GraphQL schema):**
```json
{
  "data": {
    "getFundCompareValidationSummary": {
      "validationTotal": 15,
      "validationFailed": 3,
      "validationPassed": 12,
      "validationExceptions": 5,
      "ratioTotal": 8,
      "ratioFailed": 1,
      "ratioPassed": 7,
      "validationProcessInstanceId": 123,
      "ratioProcessInstanceId": 124,
      "subchecks": [
        {
          "subtype": "Market Value",
          "status": "Completed",
          "validations": [
            {
              "validationName": "Market Value Validation",
              "description": "Validates market value calculations",
              "status": "Completed",
              "passFail": "Pass",
              "datetime": "2024-01-31T10:30:00"
            },
            {
              "validationName": "Position Count Validation",
              "description": "Ensures positions count above threshold",
              "status": "Completed",
              "passFail": "Fail",
              "datetime": "2024-01-31T10:30:00"
            }
          ]
        },
        {
          "subtype": "NAV",
          "status": "Completed",
          "validations": [
            {
              "validationName": "NAV Calculation Validation",
              "description": "Validates NAV calculations",
              "status": "Completed",
              "passFail": "Pass",
              "datetime": "2024-01-31T10:30:00"
            }
          ]
        },
        {
          "subtype": "Pricing",
          "status": "Not Completed",
          "validations": [
            {
              "validationName": "Price Comparison Validation",
              "description": "Compares prices from multiple sources",
              "status": "Not Completed",
              "passFail": "Pass",
              "datetime": null
            }
          ]
        }
      ]
    }
  }
}
```

**Note:**
- Returns `null` if no process instances match the criteria
- **Dual Source (2 sources, 1 date)**: Provide `sourceA`, `sourceB`, and `dateA`. `dateB` should be omitted or null.
- **Single Source (1 source, 2 dates)**: Provide `sourceA`, `dateA`, and `dateB`. `sourceB` should be omitted or null.
- **Validation/Ratio Counts**: 
  - `validationTotal` / `ratioTotal`: Count of unique `intvalidationconfigurationid` / `intratioconfigurationid` for the process instance
  - `validationFailed` / `ratioFailed`: Count of unique configurations with at least one failed status
  - `validationPassed` / `ratioPassed`: Total - Failed
  - `validationExceptions`: Total number of failed validation rows for that process instance
- **Subchecks**: List of validations grouped by subtype with:
  - `subtype`: The subtype name from `ValidationMaster.vcsubtype`
  - `status`: "Completed" if ALL validations in the subtype are completed, otherwise "Not Completed"
  - `validations`: List of validations with:
    - `validationName`: Name from `ValidationMaster.vcvalidationname`
    - `description`: Description from `ValidationMaster.vcdescription`
    - `status`: "Completed" if validation has results (regardless of pass/fail), otherwise "Not Completed"
    - `passFail`: "Pass" if no failed results, "Fail" if any failed results
    - `datetime`: Process instance datetime (ISO format) from `tbl_process_instance.dtcreatedat`
- **Filtering**: Only considers validations and ratios where `tbl_process_instance.vcsourcetype` matches the `vcsourcetype` in the corresponding validation/ratio masters for that process instance ID
- Subchecks are sorted by subtype name

---

