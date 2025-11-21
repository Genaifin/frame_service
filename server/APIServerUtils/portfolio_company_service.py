"""
Service for managing Portfolio Companies (Investors linked to funds)
"""
import json
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime
from database_models import Investor, FundInvestor, DatabaseManager


class PortfolioCompanyService:
    """Service class for portfolio company (investor) operations"""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
    
    async def get_portfolio_companies_by_fund(self, fund_id: int) -> Dict[str, Any]:
        """
        Get all portfolio companies under a specific fund
        
        Args:
            fund_id: Fund ID to retrieve portfolio companies for
            
        Returns:
            Dict with table configuration and row data
        """
        # Load template
        template_path = Path(__file__).parent.parent.parent / "frontendUtils" / "renders" / "view_all_portfolio_companies_of_a_fund.json"
        with open(template_path, 'r') as f:
            response = json.load(f)
        
        # Get all investors linked to this fund via fund_investors table
        session = self.db_manager.get_session()
        try:
            fund_investors = session.query(FundInvestor).filter(
                FundInvestor.fund_id == fund_id,
                FundInvestor.is_active == True
            ).all()
            
            row_data = []
            for fi in fund_investors:
                investor = session.query(Investor).filter(
                    Investor.id == fi.investor_id,
                    Investor.is_active == True
                ).first()
                
                if investor:
                    # Format investment date
                    investment_date_str = ""
                    if fi.investment_date:
                        investment_date_str = fi.investment_date.strftime("%d/%m/%Y")
                    
                    # Parse sector and geography (country) - they're stored as JSON arrays
                    sector_str = ""
                    if investor.sector:
                        try:
                            sector_list = json.loads(investor.sector) if isinstance(investor.sector, str) else investor.sector
                            sector_str = ", ".join(sector_list) if isinstance(sector_list, list) else str(sector_list)
                        except:
                            sector_str = investor.sector
                    
                    geography_str = ""
                    if investor.country:
                        try:
                            geo_list = json.loads(investor.country) if isinstance(investor.country, str) else investor.country
                            geography_str = ", ".join(geo_list) if isinstance(geo_list, list) else str(geo_list)
                        except:
                            geography_str = investor.country
                    
                    row_data.append({
                        "portfolioCompanyID": investor.id,
                        "portfolioCompanyName": investor.investor_name or "",
                        "sector": sector_str,
                        "geography": geography_str,
                        "investmentDate": investment_date_str,
                        "status": investor.status or ""
                    })
        finally:
            session.close()
        
        response["rowData"] = row_data
        return response
    
    async def add_portfolio_companies(self, fund_id: int, companies: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Add new portfolio companies (investors) to a fund (bulk create)
        
        Args:
            fund_id: Fund ID to link portfolio companies to
            companies: List of portfolio company data
            
        Returns:
            Dict with success status and created investor IDs
        """
        investor_ids = []
        session = self.db_manager.get_session()
        try:
            for company in companies:
                # Convert sector and geography lists to JSON strings
                sector_json = json.dumps(company["sector"]) if isinstance(company["sector"], list) else company["sector"]
                geography_json = json.dumps(company["geography"]) if isinstance(company["geography"], list) else company["geography"]
                
                # Generate unique account_number using timestamp
                import time
                account_number = f"PC{int(time.time() * 1000)}"
                
                # Create investor
                investor = Investor(
                    investor_name=company["company_name"],
                    account_name=company["company_name"],  # Set to company name
                    account_number=account_number,  # Generate unique account number
                    sector=sector_json,  # Sector stored in dedicated sector column
                    country=geography_json,  # Geography stored in country
                    status=company["status"],
                    is_active=True
                )
                session.add(investor)
                session.flush()  # Get the investor ID
                
                # Create fund-investor link
                fund_investor = FundInvestor(
                    fund_id=fund_id,
                    investor_id=investor.id,
                    investment_date=company["investment_date"],
                    is_active=True
                )
                session.add(fund_investor)
                investor_ids.append(investor.id)
            
            session.commit()
        finally:
            session.close()
        
        return {
            "success": True,
            "message": f"Successfully added {len(investor_ids)} portfolio companies to fund",
            "investor_ids": investor_ids
        }
    
    async def edit_portfolio_company(self, investor_id: int, companies: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Edit a portfolio company (investor)
        
        Args:
            investor_id: Investor ID to edit
            companies: List with single portfolio company data (matching request format)
            
        Returns:
            Dict with success status
        """
        session = self.db_manager.get_session()
        try:
            investor = session.query(Investor).filter(
                Investor.id == investor_id,
                Investor.is_active == True
            ).first()
            
            if not investor:
                return {
                    "success": False,
                    "message": f"Portfolio company with ID {investor_id} not found"
                }
            
            # Update from first item in companies list
            company = companies[0]
            
            # Convert lists to JSON
            sector_json = json.dumps(company["sector"]) if isinstance(company["sector"], list) else company["sector"]
            geography_json = json.dumps(company["geography"]) if isinstance(company["geography"], list) else company["geography"]
            
            investor.investor_name = company["company_name"]
            investor.sector = sector_json
            investor.country = geography_json
            investor.status = company["status"]
            
            # Update investment date in fund_investors table
            fund_investor = session.query(FundInvestor).filter(
                FundInvestor.investor_id == investor_id,
                FundInvestor.is_active == True
            ).first()
            
            if fund_investor:
                fund_investor.investment_date = company["investment_date"]
            
            session.commit()
            
            return {
                "success": True,
                "message": f"Successfully updated portfolio company '{company['company_name']}'"
            }
        finally:
            session.close()
    
    async def get_portfolio_company_details(self, investor_id: int) -> Dict[str, Any]:
        """
        Get portfolio company details
        
        Args:
            investor_id: Investor ID to retrieve details for
            
        Returns:
            Dict with portfolio company details
        """
        session = self.db_manager.get_session()
        try:
            investor = session.query(Investor).filter(
                Investor.id == investor_id,
                Investor.is_active == True
            ).first()
            
            if not investor:
                return {
                    "error": f"Portfolio company with ID {investor_id} not found"
                }
            
            # Load template
            template_path = Path(__file__).parent.parent.parent / "frontendUtils" / "renders" / "portfolio_company_details.json"
            with open(template_path, 'r') as f:
                response = json.load(f)
            
            # Get fund_investor link for investment date
            fund_investor = session.query(FundInvestor).filter(
                FundInvestor.investor_id == investor_id,
                FundInvestor.is_active == True
            ).first()
            
            investment_date_str = ""
            if fund_investor and fund_investor.investment_date:
                investment_date_str = fund_investor.investment_date.strftime("%d/%m/%Y")
            
            # Parse sector and geography
            sector_str = ""
            if investor.sector:
                try:
                    sector_list = json.loads(investor.sector) if isinstance(investor.sector, str) else investor.sector
                    sector_str = ", ".join(sector_list) if isinstance(sector_list, list) else str(sector_list)
                except:
                    sector_str = investor.sector
            
            geography_str = ""
            if investor.country:
                try:
                    geo_list = json.loads(investor.country) if isinstance(investor.country, str) else investor.country
                    geography_str = ", ".join(geo_list) if isinstance(geo_list, list) else str(geo_list)
                except:
                    geography_str = investor.country
            
            # Populate response
            portfolio_company_id = investor.id
            
            response["onEditClick"]["parameters"][0]["value"] = portfolio_company_id
            response["sections"][0]["fields"] = [
                {
                    "label": "Portfolio Company ID",
                    "value": portfolio_company_id
                },
                {
                    "label": "Portfolio Company Name",
                    "value": investor.investor_name or ""
                },
                {
                    "label": "Sector",
                    "value": sector_str
                },
                {
                    "label": "Geography",
                    "value": geography_str
                },
                {
                    "label": "Investment Date",
                    "value": investment_date_str
                },
                {
                    "label": "Status",
                    "value": investor.status or "",
                    "type": "status-badge"
                }
            ]
            
            return response
        finally:
            session.close()

