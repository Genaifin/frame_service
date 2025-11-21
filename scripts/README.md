# NexBridge Data Ingestion Scripts

This folder contains scripts for ingesting NexBridge Excel data into the database.

## Files

### 1. `nexbridge_data_ingestion.py`
Main data ingestion script that:
- Reads NexBridge Excel files from the configured folder
- Extracts data from Trial Balance, Portfolio Valuation, and Dividend sheets
- Populates the nexbridge database tables following the configuration in NexBridge.json
- Handles version management and duplicate prevention

## Configuration

The ingestion follows the NexBridge.json configuration:
- **File Identifier**: "NexBridge Global Ventures NAV PACK - Admin Bluefield Investor Services"
- **Skip Rows**: 10 rows (topRowsWithExtraData)
- **Filters**:
  - Trial Balance: Excludes Type = 'Capital'
  - Portfolio Valuation: Excludes Inv Type = 'CASH'

## Data Processing

### Trial Balance Sheet
Maps columns:
- `Type` → `type`
- `Category` → `category` 
- `Accounting Head` → `accounting_head`
- `Financial Account` → `financial_account`
- `Ending Balance` → `ending_balance`

### Portfolio Valuation Sheet
Maps columns:
- `Inv Type` → `inv_type`
- `Inv Id` → `inv_id`
- `End Qty` → `end_qty`
- `End Local Market Price` → `end_local_market_price`
- `End Local MV` → `end_local_mv`

### Dividend Sheet
Maps columns:
- `Security Id` → `security_id`
- `Security Name/Desc` → `security_name`
- `Amount` → `amount`

## Usage

### Quick Setup (Recommended)
```bash
python scripts/run_nexbridge_setup.py
```

### Manual Steps
1. Create database tables:
```bash
alembic upgrade head
```

2. Run data ingestion:
```bash
python scripts/nexbridge_data_ingestion.py
```

3. Test and verify:
```bash
python scripts/test_ingestion.py
```

## Database Schema

The scripts populate these tables:
- `nexbridge.source` - Data sources (e.g., "Bluefield")
- `nexbridge.nav_pack` - Logical grouping by fund/source/date
- `nexbridge.navpack_version` - File versions with metadata
- `nexbridge.trial_balance` - Trial balance data
- `nexbridge.portfolio_valuation` - Portfolio valuation data
- `nexbridge.dividend` - Dividend data

## Versioning

The system automatically handles versioning:
- Each file upload creates a new version
- Versions are auto-incremented per nav pack
- Full audit trail maintained with timestamps and users

## Error Handling

The scripts include comprehensive error handling:
- Graceful handling of missing sheets
- Data validation and type conversion
- Transaction rollback on errors
- Detailed logging and progress reporting

