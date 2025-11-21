import json
import os
import glob
import re
from datetime import datetime

def extractDataFromContent(content):
    """Extract all data types from content in one pass"""
    data = {'account_info': {}, 'holdings': [], 'transactions': []}
    
    # Account info extraction
    patterns = {
        'fund_name': r'METROPOLITAN JEWISH HEALTH SYSTEM\s+FOUNDATION.*?FUND',
        'account_number': r'ACCOUNT NO\.\s*([A-Z0-9\-]+)',
        'period': r'(\d{2}/\d{2}/\d{2})\s+THROUGH\s+(\d{2}/\d{2}/\d{2})',
        'account_manager': r'ACCOUNT MANAGER:\s*([A-Z\s]+)',
        'portfolio_manager': r'PORTFOLIO MANAGER:\s*([A-Z\s\.]+)'
    }
    
    for key, pattern in patterns.items():
        match = re.search(pattern, content, re.DOTALL if key == 'fund_name' else 0)
        if match:
            if key == 'period':
                data['account_info'][key] = f"{match.group(1)} through {match.group(2)}"
            elif key in ['account_manager', 'portfolio_manager']:
                data['account_info'][key] = re.sub(r'\s+', ' ', match.group(1)).strip()
            else:
                value = match.group(1) if key != 'fund_name' else match.group(0)
                data['account_info'][key] = re.sub(r'\s+', ' ', value).strip()
    
    # Portfolio holdings extraction
    if 'PORTFOLIO APPRAISAL' in content:
        lines = content.split('\n')
        for i, line in enumerate(lines):
            if any(h in line for h in ['PAR VALUE', 'NO. SHARES', 'CUSIP']):
                continue
            match = re.match(r'\s*([0-9,\.]+)\s+([A-Z0-9\-]+)\s+([0-9,\.]+)\s+([0-9,\.]+)\s+([0-9,\.\-]+)\s+([0-9,\.]+)\s+([0-9,\.]+)\s+([0-9,\.]+)', line)
            if match:
                desc = lines[i + 1].strip() if i + 1 < len(lines) and not re.match(r'^\s*\d', lines[i + 1]) else ""
                data['holdings'].append({
                    "SecurityType": "Bond", "ParValue": match.group(1).replace(',', ''),
                    "CUSIP": match.group(2), "Description": desc,
                    "AccruedInterest": match.group(3).replace(',', ''),
                    "AcquisitionCost": match.group(4).replace(',', ''),
                    "UnrealizedGainLoss": match.group(5).replace(',', ''),
                    "MarketPrice": match.group(6), "PortfolioPercentage": match.group(7),
                    "Yield": match.group(8)
                })
    
    # Transaction extraction
    if 'TRANSACTION LEDGER' in content:
        lines = content.split('\n')
        for i, line in enumerate(lines):
            match = re.match(r'\s*(\d{2}/\d{2}/\d{2})\s+(\d{2}/\d{2}/\d{2})\s+(PURCHASED|SOLD)\s+([0-9,]+)', line)
            if match:
                cusip = desc = cash = ""
                for j in range(i + 1, min(i + 5, len(lines))):
                    next_line = lines[j].strip()
                    if not cusip and re.search(r'[A-Z0-9\-]{8,}', next_line):
                        cusip = re.search(r'([A-Z0-9\-]{8,})', next_line).group(1)
                    if 'TREASURY' in next_line:
                        desc = next_line
                    if not cash and re.search(r'([0-9,]+\.\d{2})', next_line):
                        cash = re.search(r'([0-9,]+\.\d{2})', next_line).group(1)
                data['transactions'].append({
                    "TradeDate": match.group(1), "SettlementDate": match.group(2),
                    "Action": match.group(3), "Amount": match.group(4).replace(',', ''),
                    "CUSIP": cusip, "Description": desc, "CashAmount": cash
                })
    
    return data

def processLongFormDoc():
    """Process long form document with rich data extraction"""
    long_form_dir = "data/frameDemo/l1/1a4f7efda18d50621d318d6f4ae479f5275f090d3658d5d419ac4e6c45c9a84b"
    page_files = sorted(glob.glob(f"{long_form_dir}/page_*_fitz.md"))
    
    pages_data, total_words, all_holdings, all_transactions, all_content = {}, 0, [], [], ""
    account_info = {}
    
    # Process all pages
    for page_file in page_files:
        page_num = os.path.basename(page_file).split('_')[1]
        with open(page_file, "r") as f:
            content = f.read()
        
        all_content += content + "\n"
        words = len(content.split())
        total_words += words
        
        # Extract data
        if page_num == "000":
            account_info = extractDataFromContent(content)['account_info']
        
        page_data = extractDataFromContent(content)
        all_holdings.extend(page_data['holdings'])
        all_transactions.extend(page_data['transactions'])
        
        pages_data[f"page_{page_num}"] = {
            "page_number": int(page_num) + 1, "content": content, "word_count": words,
            "holdings_found": len(page_data['holdings']), "transactions_found": len(page_data['transactions'])
        }
    
    # Financial summary
    financial_summary = {}
    for pattern, key in [
        (r'TOTAL.*?([0-9,]+\.\d{2})', 'TotalPortfolioValue'),
        (r'NET ASSETS.*?([0-9,]+\.\d{2})', 'NetAssets'),
        (r'MARKET VALUE.*?([0-9,]+\.\d{2})', 'TotalMarketValue'),
        (r'CASH.*?([0-9,]+\.\d{2})', 'CashBalance')
    ]:
        match = re.search(pattern, all_content, re.IGNORECASE)
        if match:
            financial_summary[key] = match.group(1).replace(',', '')
    
    # Build entities
    portfolio_entities = {}
    entity_configs = [
        ("DocumentInfo", account_info.get('fund_name', '013.pdf'), account_info.get('fund_name', ''), 1),
        ("AccountNumber", account_info.get('account_number', 'Not Found'), f"ACCOUNT NO. {account_info.get('account_number', '')}", 1),
        ("ReportingPeriod", account_info.get('period', 'Not Found'), f"FOR THE PERIOD {account_info.get('period', '').upper()}", 1),
        ("AccountManager", account_info.get('account_manager', 'Not Found'), f"ACCOUNT MANAGER: {account_info.get('account_manager', '')}", 1),
        ("PortfolioManager", account_info.get('portfolio_manager', 'Not Found'), f"PORTFOLIO MANAGER: {account_info.get('portfolio_manager', '')}", 1),
        ("TotalHoldings", f"{len(all_holdings)} securities", f"Portfolio contains {len(all_holdings)} individual securities", "Multiple"),
        ("TotalTransactions", f"{len(all_transactions)} transactions", f"Transaction ledger shows {len(all_transactions)} transactions", "Multiple"),
        ("DocumentStructure", "Portfolio Appraisal, Transaction Ledger, Bond Maturity Summary", "Document contains detailed portfolio appraisal and transaction history", 2)
    ]
    
    for key, value, verbatim, page in entity_configs:
        portfolio_entities[key] = {"Value": value, "ConfidenceScore": "HIGH", "VerbatimText": verbatim, "PageNumber": page}
    
    # Add financial summary
    for key, value in financial_summary.items():
        portfolio_entities[key] = {"Value": f"${value}", "ConfidenceScore": "HIGH", "VerbatimText": f"Financial summary shows {key}: ${value}", "PageNumber": "Multiple"}
    
    # Document details
    doc_details = {
        "document_name": "013.pdf", "date": datetime.now().strftime("%Y-%m-%d"), "status": "PROCESSED",
        "source": "Long Form", "processing_method": "Automated", "file_type": "Fund Statement",
        "extractor": "Aithon Frame - AI", "total_pages": len(page_files),
        "fund_name": account_info.get('fund_name', 'N/A'), "account_number": account_info.get('account_number', 'N/A'),
        "reporting_period": account_info.get('period', 'N/A'), "account_manager": account_info.get('account_manager', 'N/A'),
        "portfolio_manager": account_info.get('portfolio_manager', 'N/A'), "total_holdings": len(all_holdings),
        "total_transactions": len(all_transactions), "total_words": total_words,
        "processing_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "document_sections": "Portfolio Appraisal, Transaction Ledger, Bond Maturity",
        "data_quality": "HIGH" if len(all_holdings) > 50 else "MEDIUM",
        "extraction_coverage": f"{len(page_files)} pages processed"
    }
    
    # Frontend data structure
    frontend_data = {
        "document_details": doc_details,
        "file_metadata_config": [
            {"key": "status", "label": "Status", "layout": "badge"},
            {"key": "source", "label": "Source"}, {"key": "file_type", "label": "File Type"},
            {"key": "fund_name", "label": "Fund Name"}, {"key": "account_number", "label": "Account Number"},
            {"key": "reporting_period", "label": "Reporting Period"}, {"key": "account_manager", "label": "Account Manager"},
            {"key": "portfolio_manager", "label": "Portfolio Manager"}, {"key": "total_pages", "label": "Total Pages"},
            {"key": "total_holdings", "label": "Portfolio Holdings"}, {"key": "total_transactions", "label": "Total Transactions"},
            {"key": "processing_method", "label": "Processing Method"}, {"key": "extractor", "label": "Extractor"},
            {"key": "data_quality", "label": "Data Quality", "layout": "badge"},
            {"key": "extraction_coverage", "label": "Coverage"}, {"key": "processing_date", "label": "Processing Date"}
        ],
        "extracted_data": {"entities": [{"portfolio": [portfolio_entities]}]},
        "account_details": {
            "account_sid": account_info.get('account_number', 'N/A'),
            "fund_name": account_info.get('fund_name', 'Long Form Document'),
            "account_manager": account_info.get('account_manager', 'N/A'),
            "portfolio_manager": account_info.get('portfolio_manager', 'N/A'),
            "reporting_period": account_info.get('period', 'N/A'),
            "total_pages": len(page_files), "portfolio_holdings": len(all_holdings), "total_transactions": len(all_transactions)
        },
        "account_fields": [{"key": k, "label": l} for k, l in [
            ("fund_name", "Fund Name"), ("account_sid", "Account Number"), ("account_manager", "Account Manager"),
            ("portfolio_manager", "Portfolio Manager"), ("reporting_period", "Reporting Period"), ("total_pages", "Total Pages"),
            ("portfolio_holdings", "Portfolio Holdings"), ("total_transactions", "Total Transactions")
        ]],
        "pages": pages_data, "portfolio_holdings": all_holdings[:20], "recent_transactions": all_transactions[:15],
        "financial_summary": financial_summary
    }
    
    # Write files
    with open(f"{long_form_dir}/forFrontend.json", "w") as f:
        json.dump(frontend_data, f, indent=2)
    
    with open(f"{long_form_dir}/fileMetaData.json", "w") as f:
        json.dump({
            "fileName": "013.pdf", "fileHash": "1a4f7efda18d50621d318d6f4ae479f5275f090d3658d5d419ac4e6c45c9a84b", "fileType": "Fund Statement",
            "status": "processed", "dateProcessed": datetime.now().isoformat(),
            "totalPages": len(page_files), "extractedHoldings": len(all_holdings), "extractedTransactions": len(all_transactions)
        }, f, indent=2)
    
    # Update allFileMeta.json and l2.json
    for file_path, data_key, data_value in [
        ("data/frameDemo/ldummy/allFileMeta.json", "013.pdf", {"fileHash": "1a4f7efda18d50621d318d6f4ae479f5275f090d3658d5d419ac4e6c45c9a84b", "fileType": "Fund Statement", "status": "Processed", "fileName": "013.pdf"}),
        ("data/frameDemo/states/l2.json", "processedFiles", "013.pdf")
    ]:
        try:
            with open(file_path, "r") as f:
                data = json.load(f)
            if file_path.endswith("allFileMeta.json"):
                data[data_key] = data_value
            else:
                if data_value not in data.get(data_key, []):
                    data.setdefault(data_key, []).append(data_value)
            with open(file_path, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"Warning: Could not update {file_path} - {e}")
    
    print("Long form document processing completed")
    print(f"Rich Extracted Data Summary:")
    for label, value in [
        ("Fund", account_info.get('fund_name', 'N/A')), ("Account", account_info.get('account_number', 'N/A')),
        ("Manager", account_info.get('account_manager', 'N/A')), ("Period", account_info.get('period', 'N/A')),
        ("Portfolio Holdings", f"{len(all_holdings)} securities"), ("Transactions", f"{len(all_transactions)} trades"),
        ("Financial Summary", f"{len(financial_summary)} metrics"), ("Pages", len(page_files)), ("Words", total_words)
    ]:
        print(f"  - {label}: {value}")

if __name__ == "__main__":
    processLongFormDoc()