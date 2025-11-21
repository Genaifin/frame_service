# Multi-Fund Data Ingestion Guide

The `nexbridge_data_ingestion.py` script now supports multiple funds with auto-detection and configurable settings.

## Supported Funds

| Fund | ID | Default Source | File Identifier Keywords |
|------|----|----|---------------------------|
| **NexBridge** | 1 | Bluefield | "NexBridge Global Ventures" |
| **ASOF** | 2 | Harborview | "Altura Strategic Opportunities" |
| **Stonewell** | 3 | StratusGA | "Stonewell Diversified" |

## Usage Examples

### 1. List Available Funds
```bash
python scripts/nexbridge_data_ingestion.py --list-funds
```

### 2. Process Specific Fund
```bash
# Process NexBridge with default settings



# Process ASOF with ClearLedger source override
python scripts/nexbridge_data_ingestion.py --fund ASOF --source ClearLedger

# Process Stonewell with custom data folder
python scripts/nexbridge_data_ingestion.py --fund Stonewell --data-folder /path/to/data
```

### 3. Auto-Detection Mode (Recommended)
```bash
# Auto-detect and process all funds
python scripts/nexbridge_data_ingestion.py --auto-detect

# Auto-detect with custom data folder
python scripts/nexbridge_data_ingestion.py --auto-detect --data-folder /custom/path
```

## Features

### ✅ **Auto-Detection**
- **Fund Detection**: Automatically identifies fund from filename keywords
- **Source Detection**: Auto-detects source (Harborview, ClearLedger, Bluefield, StratusGA, etc.)
- **Multi-Source Support**: Handles both Admin and Shadow sources per fund

### ✅ **Flexible Configuration**
- **Per-Fund Settings**: Each fund has its own configuration (ID, default source, file patterns)
- **Source Override**: Can override default source per fund
- **Custom Data Paths**: Support for custom data folder locations

### ✅ **Database Integration**
- **Auto-Source Creation**: Automatically creates missing sources in `nexbridge.source`
- **Fund ID Mapping**: Uses correct fund IDs (NexBridge=1, ASOF=2, Stonewell=3)
- **Version Management**: Handles multiple versions per nav pack

### ✅ **Error Handling**
- **Graceful Failures**: Continues processing other files if one fails
- **Detailed Logging**: Shows which fund/source was detected for each file
- **Summary Reports**: Provides success/failure counts

## Auto-Detection Logic

### Fund Detection Priority:
1. **File Identifier Match**: Checks for fund-specific identifiers in filename
2. **Keyword Search**: Searches for fund names/codes in filename
3. **Fallback**: Uses current fund configuration if no match

### Source Detection:
- **Per-Fund Mapping**: Each fund has specific source keywords
- **Filename Parsing**: Looks for source names in filename
- **Default Fallback**: Uses fund's default source if not detected

## File Naming Conventions

For best auto-detection results, include both fund and source in filename:

```
✅ Good Examples:
- "NexBridge Global Ventures NAV PACK - Admin Bluefield - Jan 2024.xlsx"
- "Altura Strategic Opportunities - Harborview - Feb 2024.xlsx" 
- "ASOF NAV PACK - Shadow ClearLedger - Mar 2024.xlsx"
- "Stonewell Diversified - StratusGA - Apr 2024.xlsx"

⚠️ Will Work (but less precise):
- "ASOF_data_Jan2024.xlsx"
- "Stonewell_Feb2024.xlsx"
```

## Database Schema

Sources are automatically created in `nexbridge.source` table:
- Harborview, ClearLedger (ASOF sources)
- Bluefield (NexBridge source)  
- StratusGA, VeridexAS (Stonewell sources)

Nav packs are created with proper fund IDs and linked to auto-detected sources.

## Programmatic Usage

```python
from scripts.nexbridge_data_ingestion import NexBridgeDataIngestion

# Single fund processing
ingestion = NexBridgeDataIngestion(fund_config='ASOF')
ingestion.set_source_override('ClearLedger')
ingestion.run_ingestion()

# Multi-fund auto-detection
ingestion = NexBridgeDataIngestion()
ingestion.run_multi_fund_ingestion()

# Helper functions
from scripts.nexbridge_data_ingestion import run_specific_fund, run_all_funds

run_specific_fund('ASOF', 'ClearLedger')
run_all_funds('/path/to/data')
```

## Migration from Original Script

The enhanced script is **100% backward compatible**:
- Original usage still works (defaults to NexBridge)
- All existing file processing logic preserved
- Same database schema and relationships
- No breaking changes to existing workflows

Simply replace calls to the original script with the new enhanced version for multi-fund support!
