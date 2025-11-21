from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime


class PortfolioCompanyItem(BaseModel):
    """Single portfolio company item in bulk create"""
    company_name: str = Field(..., description="Portfolio company name")
    sector: List[str] = Field(..., description="Sectors (e.g., Energy, Materials)")
    geography: List[str] = Field(..., description="Geographic locations (e.g., USA, India)")
    investment_date: datetime = Field(..., description="Investment date")
    status: str = Field(..., description="Status (e.g., invested, exited)")


class AddPortfolioCompanyRequest(BaseModel):
    """Request model for adding portfolio companies to a fund"""
    portfolio_companies: List[PortfolioCompanyItem] = Field(..., description="List of portfolio companies to add")


class EditPortfolioCompanyRequest(BaseModel):
    """Request model for editing a portfolio company"""
    portfolio_companies: List[PortfolioCompanyItem] = Field(..., description="List of portfolio companies to update")


class PortfolioCompanyResponse(BaseModel):
    """Response model for portfolio company operations"""
    success: bool
    message: str
    investor_ids: Optional[List[int]] = None

