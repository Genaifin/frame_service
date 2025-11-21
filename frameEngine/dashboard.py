"""
Smart Dashboard Generator for Aithon Framework
Single function to generate comprehensive dashboard JSON with auto-detection
"""

import json
import os
import time
import psutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional


def generate_dashboard(output_file: str = "output_documents/dashboard1.json") -> Dict[str, Any]:
    """
    Smart dashboard generation - auto-detects everything and generates comprehensive JSON
    
    Args:
        output_file: Output file name (default: "dashboard.json")
        
    Returns:
        Complete dashboard data dictionary
    """
    
    # Auto-detect directories
    source_dir = Path("source_documents")
    output_dir = Path("output_documents")
    
    # Get system info
    system_info = {
        "cpu_usage": psutil.cpu_percent(interval=0.1),
        "memory_usage": psutil.virtual_memory().percent,
        "disk_usage": psutil.disk_usage('.').percent,
        "timestamp": datetime.now().isoformat()
    }
    
    # Get file processing status
    source_files = list(source_dir.glob("*.pdf")) if source_dir.exists() else []
    output_files = list(output_dir.glob("*_output.json")) if output_dir.exists() else []
    
    # Parse output files for processing details
    files_details = []
    for output_file_path in output_files:
        try:
            with open(output_file_path, 'r') as f:
                file_data = json.load(f)
                
            # Extract key information
            file_detail = {
                "file_id": file_data.get("file_id", "unknown"),
                "filename": file_data.get("filename", output_file_path.stem.replace("_output", "")),
                "document_type": file_data.get("document_type", "unknown"),
                "classification_confidence": file_data.get("classification_confidence", 0.0),
                "processing_status": "SUCCESS" if file_data.get("extracted_data") else "FAILED",
                "events_count": len(file_data.get("events_log", [])),
                "file_size": output_file_path.stat().st_size,
                "processed_at": datetime.fromtimestamp(output_file_path.stat().st_mtime).isoformat()
            }
            files_details.append(file_detail)
        except Exception:
            # If file can't be read, mark as failed
            files_details.append({
                "filename": output_file_path.stem.replace("_output", ""),
                "processing_status": "FAILED",
                "error": "Could not read output file"
            })
    
    # Calculate statistics
    total_files = len(source_files)
    successful_files = len([f for f in files_details if f.get("processing_status") == "SUCCESS"])
    failed_files = len([f for f in files_details if f.get("processing_status") == "FAILED"])
    success_rate = (successful_files / total_files * 100) if total_files > 0 else 0
    
    # Document type distribution
    doc_types = {}
    for file_detail in files_details:
        doc_type = file_detail.get("document_type", "unknown")
        doc_types[doc_type] = doc_types.get(doc_type, 0) + 1
    
    # Confidence distribution
    confidence_levels = {"HIGH": 0, "MEDIUM": 0, "LOW": 0}
    for file_detail in files_details:
        confidence = file_detail.get("classification_confidence", 0.0)
        if confidence >= 0.9:
            confidence_levels["HIGH"] += 1
        elif confidence >= 0.8:
            confidence_levels["MEDIUM"] += 1
        else:
            confidence_levels["LOW"] += 1
    
    # Generate alerts
    alerts = []
    if system_info["cpu_usage"] > 80:
        alerts.append({"type": "WARNING", "message": f"High CPU usage: {system_info['cpu_usage']:.1f}%"})
    if system_info["memory_usage"] > 85:
        alerts.append({"type": "WARNING", "message": f"High memory usage: {system_info['memory_usage']:.1f}%"})
    if failed_files > 0:
        alerts.append({"type": "ERROR", "message": f"{failed_files} files failed processing"})
    if success_rate < 90 and total_files > 0:
        alerts.append({"type": "WARNING", "message": f"Low success rate: {success_rate:.1f}%"})
    
    # Build complete dashboard
    dashboard = {
        "dashboard_metadata": {
            "generated_at": datetime.now().isoformat(),
            "version": "1.0",
            "generator": "Smart Dashboard"
        },
        "system_overview": {
            "system_health": "HEALTHY" if len([a for a in alerts if a["type"] == "ERROR"]) == 0 else "UNHEALTHY",
            "total_documents": total_files,
            "successful_documents": successful_files,
            "failed_documents": failed_files,
            "success_rate": round(success_rate, 2),
            "cpu_usage": system_info["cpu_usage"],
            "memory_usage": system_info["memory_usage"],
            "disk_usage": system_info["disk_usage"]
        },
        "files_processing_details": files_details,
        "statistics": {
            "document_types": doc_types,
            "confidence_distribution": confidence_levels,
            "total_events": sum(f.get("events_count", 0) for f in files_details)
        },
        "alerts": alerts,
        "system_metrics": system_info
    }
    
    # Save to file
    try:
        with open(output_file, 'w') as f:
            json.dump(dashboard, f, indent=2)
        print(f"✅ Dashboard generated: {output_file}")
    except Exception as e:
        print(f" Failed to save dashboard: {e}")
    
    return dashboard


def get_status() -> Dict[str, Any]:
    """Quick system status check"""
    dashboard = generate_dashboard("/tmp/temp_dashboard.json")
    return dashboard["system_overview"]


def get_files_summary() -> Dict[str, Any]:
    """Quick files processing summary"""
    dashboard = generate_dashboard("output_documents/dashboard1.json")
    return {
        "total_files": dashboard["system_overview"]["total_documents"],
        "successful_files": dashboard["system_overview"]["successful_documents"],
        "failed_files": dashboard["system_overview"]["failed_documents"],
        "success_rate": dashboard["system_overview"]["success_rate"]
    }


# Simple usage examples
if __name__ == "__main__":
    print("🚀 Smart Dashboard Generator")
    
    # Generate full dashboard
    dashboard_data = generate_dashboard()
    
    # Quick status check
    status = get_status()
    print(f"📊 System Status: {status['system_health']}")
    print(f"📁 Files: {status['successful_documents']}/{status['total_documents']} successful")
    
    # Quick files summary
    summary = get_files_summary()
    print(f"📈 Success Rate: {summary['success_rate']}%") 