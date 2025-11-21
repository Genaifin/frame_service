# 🚀 Automated Dashboard System - Usage Guide

## Overview
The Aithon Framework now includes a fully automated dashboard system that captures real-time processing information and generates comprehensive JSON reports for your monitoring needs.

## 🎯 Key Features

### ✅ **Fully Automated**
- **No manual input required** - automatically captures logs and metrics
- **Real-time system monitoring** - CPU, memory, disk usage
- **File processing status** - tracks all source and output files
- **Multiple data sources** - logs, metrics, system info, file status

### 📊 **Comprehensive Data**
- **System Overview** - health status, success rates, throughput
- **Individual File Details** - processing time, quality scores, confidence levels
- **Performance Metrics** - fastest/slowest documents, average processing times
- **Quality Distribution** - excellent/good/acceptable/poor quality breakdown
- **Alert System** - system warnings and processing issues

## 🔧 How to Use

### 1. **Automatic Generation (Recommended)**
The orchestrator now automatically generates the dashboard after processing:

```bash
python3 orchestrator.py
# Dashboard automatically generated as 'dashboard.json'
```

### 2. **Manual Generation**
Generate dashboard anytime:

```bash
python3 generate_dashboard.py
```

### 3. **Function Call Integration**
Use from within your Python code:

```python
from dashboard_api import generate_dashboard

# Simple usage
dashboard = generate_dashboard()

# Custom paths
dashboard = generate_dashboard(
    output_file="my_dashboard.json",
    log_file="custom_logs.log",
    metrics_file="custom_metrics.json"
)
```

### 4. **API-Style Functions**
Quick access to specific data:

```python
from dashboard_api import (
    get_system_status,
    get_file_processing_summary,
    dashboard_api_status,
    dashboard_api_files,
    dashboard_api_full
)

# Get just system status
status = get_system_status()
print(f"System health: {status['system_health']}")

# Get file processing summary
summary = get_file_processing_summary()
print(f"Total files: {summary['total_files']}")

# API-style responses (perfect for web dashboards)
api_response = dashboard_api_full()
```

## 📋 Dashboard JSON Structure

### **System Overview**
```json
{
  "system_overview": {
    "system_health": "HEALTHY",
    "total_documents": 3,
    "successful_documents": 3,
    "success_rate_percent": 100.0,
    "total_processing_time_seconds": 89.21,
    "average_processing_time_seconds": 29.74,
    "throughput_docs_per_hour": 121.06
  }
}
```

### **Individual File Details**
```json
{
  "files_processing_details": [
    {
      "file_info": {
        "file_id": "da1e944b-0fd9-4d92-aca7-747a6a6ea9de",
        "filename": "document.pdf",
        "file_hash": "45dc303bd9cbf97a",
        "file_size_bytes": 281074,
        "file_size_mb": 0.27
      },
      "processing_status": {
        "status": "SUCCESS",
        "start_time": "2025-07-12 02:50:23,937",
        "end_time": "2025-07-12 02:51:10,330",
        "total_processing_time": 46.39,
        "stages_completed": 7,
        "events_count": 8
      },
      "classification_info": {
        "document_type": "CapCall",
        "confidence": 0.81,
        "confidence_level": "MEDIUM",
        "provider_used": "openai",
        "processing_time": 2.6,
        "retries": 0
      },
      "extraction_info": {
        "quality_level": "EXCELLENT",
        "quality_score": 1.0,
        "valid": true,
        "provider_used": "openai",
        "processing_time": 43.72,
        "retries": 0
      }
    }
  ]
}
```

### **Performance Metrics**
```json
{
  "performance_summary": {
    "fastest_document": 18.19,
    "slowest_document": 46.39,
    "average_classification_confidence": 0.823,
    "average_extraction_quality": 1.0,
    "document_types_processed": ["CapCall", "Distribution"],
    "providers_used": ["openai"]
  }
}
```

## 🔍 Data Sources

The dashboard automatically collects data from multiple sources:

### **1. Log Files**
- Automatically searches for: `orchestrator.log`, `aithon.log`, `logs/orchestrator.log`
- Parses processing events, timings, and status information
- Extracts document IDs, confidence scores, quality metrics

### **2. Metrics Files**
- Loads from: `metrics_export.json`
- System performance data, counters, histograms
- Processing statistics and error rates

### **3. System Information**
- Real-time CPU, memory, disk usage
- Process count and system health
- Collected using `psutil`

### **4. File System Status**
- Scans `source_documents/` and `output_documents/` directories
- File sizes, modification times, processing completion status
- Automatically detects new files and processing status

## 🎨 Dashboard Integration Examples

### **For Web Dashboards**
```javascript
// Fetch dashboard data
fetch('/api/dashboard')
  .then(response => response.json())
  .then(data => {
    // System health indicator
    const systemHealth = data.system_overview.system_health;
    document.getElementById('health-status').textContent = systemHealth;
    
    // Success rate chart
    const successRate = data.system_overview.success_rate_percent;
    updateSuccessRateChart(successRate);
    
    // File processing cards
    data.files_processing_details.forEach(file => {
      createFileCard(file);
    });
  });
```

### **For Monitoring Systems**
```python
# Integration with monitoring systems
def check_system_health():
    status = get_system_status()
    
    if status['system_health'] != 'HEALTHY':
        send_alert(f"System health: {status['system_health']}")
    
    if status['success_rate_percent'] < 95:
        send_alert(f"Success rate dropped to {status['success_rate_percent']}%")
```

### **For Real-time Updates**
```python
import time
from dashboard_api import update_dashboard_realtime

# Update dashboard every 30 seconds
while True:
    if update_dashboard_realtime():
        print("Dashboard updated successfully")
    else:
        print("Failed to update dashboard")
    time.sleep(30)
```

## 🚨 Alert System

The dashboard includes an intelligent alert system:

### **Automatic Alerts**
- **Document Processing Failures** - When documents fail to process
- **Low Classification Confidence** - When confidence scores drop below 80%
- **High CPU Usage** - When CPU usage exceeds 80%
- **High Memory Usage** - When memory usage exceeds 85%
- **System Warnings** - API failures, initialization issues

### **Alert Structure**
```json
{
  "system_alerts": [
    {
      "alert_type": "LOW_CLASSIFICATION_CONFIDENCE",
      "severity": "MEDIUM",
      "message": "2 documents have low classification confidence",
      "timestamp": "2025-07-12T03:30:00Z",
      "resolved": false
    }
  ]
}
```

## 📈 Performance Monitoring

### **Key Metrics Tracked**
- **Processing Speed** - Documents per hour, average processing time
- **Quality Metrics** - Classification confidence, extraction quality
- **System Resources** - CPU, memory, disk usage
- **Error Rates** - Failed documents, retry counts
- **Provider Performance** - OpenAI vs Gemini performance comparison

### **Quality Distribution**
```json
{
  "quality_distribution": {
    "excellent_quality": 3,  // ≥95% quality
    "good_quality": 0,       // 85-95% quality
    "acceptable_quality": 0, // 75-85% quality
    "poor_quality": 0        // <75% quality
  }
}
```

## 🔧 Configuration Options

### **Command Line Usage**
```bash
# With custom paths
python3 generate_dashboard.py --log-file my_logs.log --metrics-file my_metrics.json --output my_dashboard.json

# Auto-detection (recommended)
python3 generate_dashboard.py
```

### **Environment Variables**
```bash
export AITHON_LOG_FILE="custom_logs.log"
export AITHON_METRICS_FILE="custom_metrics.json"
export AITHON_DASHBOARD_FILE="custom_dashboard.json"
```

## 🎯 Best Practices

### **1. Real-time Monitoring**
- Set up periodic dashboard updates (every 30-60 seconds)
- Monitor system health and success rates
- Set up alerts for critical issues

### **2. Performance Optimization**
- Use dashboard data to identify bottlenecks
- Monitor processing times and optimize slow stages
- Track provider performance and switch if needed

### **3. Quality Assurance**
- Monitor confidence levels and quality scores
- Set up alerts for low-quality processing
- Track document type distribution

### **4. Resource Management**
- Monitor CPU and memory usage
- Set up alerts for resource exhaustion
- Plan capacity based on throughput metrics

## 🚀 Integration with Your Systems

The dashboard system is designed to integrate seamlessly with:

- **Web Dashboards** - JSON API responses
- **Monitoring Systems** - Prometheus, Grafana integration
- **Alert Systems** - Email, Slack, PagerDuty notifications
- **Analytics Platforms** - Data export for analysis
- **CI/CD Pipelines** - Automated quality checks

## 📊 Sample Dashboard Output

The system generates rich, comprehensive data perfect for creating beautiful dashboards that show:

- **Real-time system status** with health indicators
- **Individual file processing** with detailed timelines
- **Performance metrics** with charts and graphs
- **Quality distributions** with visual breakdowns
- **Alert notifications** with severity levels
- **Resource utilization** with trend analysis

Your dashboard JSON is now automatically generated and ready to power any monitoring or visualization system you choose to build! 