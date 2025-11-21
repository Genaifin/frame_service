#!/usr/bin/env python3
"""
Test script for fund filtering functionality

Usage:
    python test_fund_filtering.py
"""

import asyncio
import sys
from athena.db_operations import get_fund_id_from_name, get_available_funds
from athena.question_answering import answer_question

async def test_fund_lookup():
    """Test fund name to ID mapping"""
    print("="*80)
    print("TEST 1: Fund Name Lookup")
    print("="*80)
    
    test_cases = [
        "NexBridge",
        "nexbridge",
        "NEXBRIDGE",
        "InvalidFund",
        "Alpha Fund",
        "1"  # Test by ID
    ]
    
    for fund_name in test_cases:
        fund_id = get_fund_id_from_name(fund_name)
        status = "✅ Found" if fund_id else "❌ Not Found"
        print(f"{status}: '{fund_name}' → {fund_id}")
    
    print()

def test_available_funds():
    """Test getting available funds list"""
    print("="*80)
    print("TEST 2: Available Funds")
    print("="*80)
    
    funds = get_available_funds()
    
    if not funds:
        print("❌ No funds found in database")
        return False
    
    print(f"✅ Found {len(funds)} funds with data:\n")
    
    for fund in funds:
        print(f"Fund ID: {fund['fund_id']}")
        print(f"  Name: {fund['name']}")
        print(f"  Code: {fund['code']}")
        print(f"  Date Range: {fund['earliest_date']} to {fund['latest_date']}")
        print(f"  Months: {fund['month_count']}")
        print()
    
    return True

async def test_chatbot_with_fund():
    """Test chatbot query with fund filtering"""
    print("="*80)
    print("TEST 3: Chatbot with Fund Filtering")
    print("="*80)
    
    question = "Give me bar chart for legal fees in last 6 months"
    
    # Test 1: With fund filtering
    print("\n--- Query WITH fund filtering (NexBridge) ---")
    fund_id = get_fund_id_from_name("NexBridge")
    
    if fund_id:
        response = await answer_question(question, fund_id=fund_id, fund_name="NexBridge")
        print(f"\n✅ Response received:")
        print(f"Response structure: {list(response.keys())}")
        
        if 'response' in response:
            print(f"Text: {response['response'].get('text', 'N/A')}")
            if 'modules' in response['response']:
                print(f"Modules: {len(response['response']['modules'])}")
    else:
        print("❌ NexBridge fund not found - skipping test")
    
    # Test 2: Without fund filtering
    print("\n--- Query WITHOUT fund filtering (All funds) ---")
    response = await answer_question(question)
    print(f"\n✅ Response received:")
    print(f"Response structure: {list(response.keys())}")
    
    if 'response' in response:
        print(f"Text: {response['response'].get('text', 'N/A')}")
        if 'modules' in response['response']:
            print(f"Modules: {len(response['response']['modules'])}")
    
    print()

async def test_invalid_fund():
    """Test chatbot with invalid fund name"""
    print("="*80)
    print("TEST 4: Invalid Fund Name Handling")
    print("="*80)
    
    fund_id = get_fund_id_from_name("InvalidFund")
    
    if fund_id is None:
        print("✅ Correctly returns None for invalid fund")
        
        # Show what the API would return
        available_funds = get_available_funds()
        fund_names = [f"{f['name']} (ID: {f['fund_id']})" for f in available_funds]
        error_msg = f"Fund 'InvalidFund' not found in database. Available funds: {', '.join(fund_names)}"
        print(f"\n📝 Error message that would be returned:")
        print(f"   {error_msg}")
    else:
        print("❌ Unexpectedly found a fund ID for 'InvalidFund'")
    
    print()

async def main():
    """Run all tests"""
    print("\n" + "="*80)
    print("FUND FILTERING TEST SUITE")
    print("="*80 + "\n")
    
    try:
        # Test 1: Fund lookup
        await test_fund_lookup()
        
        # Test 2: Available funds
        has_funds = test_available_funds()
        
        if not has_funds:
            print("⚠️  No funds in database - skipping chatbot tests")
            return
        
        # Test 3: Chatbot with fund filtering
        await test_chatbot_with_fund()
        
        # Test 4: Invalid fund handling
        await test_invalid_fund()
        
        print("="*80)
        print("✅ ALL TESTS COMPLETED")
        print("="*80 + "\n")
        
    except Exception as e:
        print(f"\n❌ ERROR during testing: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())

