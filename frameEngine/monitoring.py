"""
Advanced Monitoring and Metrics System for Aithon Framework
"""

import time
import logging
import json
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
import threading
from collections import defaultdict, deque
import psutil
import os


class MetricType(Enum):
    """Types of metrics"""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    TIMER = "timer"
    SUMMARY = "summary"


class AlertLevel(Enum):
    """Alert severity levels"""
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@dataclass
class MetricData:
    """Data structure for metric information"""
    name: str
    type: MetricType
    value: float
    timestamp: datetime
    labels: Dict[str, str] = field(default_factory=dict)
    description: str = ""


@dataclass
class AlertRule:
    """Alert rule configuration"""
    name: str
    metric_name: str
    condition: str  # e.g., ">", "<", ">=", "<=", "=="
    threshold: float
    level: AlertLevel
    message: str
    enabled: bool = True
    cooldown_minutes: int = 5


class MetricsCollector:
    """Comprehensive metrics collector with advanced features"""
    
    def __init__(self, enable_system_metrics: bool = True):
        self.metrics = {}
        self.metric_history = defaultdict(lambda: deque(maxlen=1000))
        self.counters = defaultdict(float)
        self.gauges = defaultdict(float)
        self.histograms = defaultdict(list)
        self.timers = defaultdict(list)
        self.summaries = defaultdict(list)
        
        self.enable_system_metrics = enable_system_metrics
        self.alert_rules = []
        self.alert_history = []
        self.last_alert_times = {}
        
        # Performance tracking
        self.operation_times = defaultdict(list)
        self.operation_counts = defaultdict(int)
        self.error_counts = defaultdict(int)
        
        # System metrics
        if enable_system_metrics:
            self._start_system_monitoring()
    
    def _start_system_monitoring(self):
        """Start system metrics collection in background"""
        def collect_system_metrics():
            while True:
                try:
                    # CPU metrics
                    cpu_percent = psutil.cpu_percent(interval=1)
                    self.set_gauge("system_cpu_usage_percent", cpu_percent)
                    
                    # Memory metrics
                    memory = psutil.virtual_memory()
                    self.set_gauge("system_memory_usage_percent", memory.percent)
                    self.set_gauge("system_memory_used_bytes", memory.used)
                    self.set_gauge("system_memory_available_bytes", memory.available)
                    
                    # Disk metrics
                    disk = psutil.disk_usage('/')
                    self.set_gauge("system_disk_usage_percent", disk.percent)
                    self.set_gauge("system_disk_used_bytes", disk.used)
                    self.set_gauge("system_disk_free_bytes", disk.free)
                    
                    # Process metrics
                    process = psutil.Process()
                    self.set_gauge("process_memory_rss_bytes", process.memory_info().rss)
                    self.set_gauge("process_memory_vms_bytes", process.memory_info().vms)
                    self.set_gauge("process_cpu_percent", process.cpu_percent())
                    
                    time.sleep(30)  # Collect every 30 seconds
                except Exception as e:
                    logging.error(f"Error collecting system metrics: {e}")
                    time.sleep(60)  # Wait longer on error
        
        thread = threading.Thread(target=collect_system_metrics, daemon=True)
        thread.start()
    
    def increment_counter(self, name: str, value: float = 1.0, labels: Dict[str, str] = None):
        """Increment a counter metric"""
        labels = labels or {}
        key = f"{name}_{self._labels_to_string(labels)}"
        self.counters[key] += value
        self._record_metric(name, MetricType.COUNTER, self.counters[key], labels)
    
    def set_gauge(self, name: str, value: float, labels: Dict[str, str] = None):
        """Set a gauge metric"""
        labels = labels or {}
        key = f"{name}_{self._labels_to_string(labels)}"
        self.gauges[key] = value
        self._record_metric(name, MetricType.GAUGE, value, labels)
    
    def observe_histogram(self, name: str, value: float, labels: Dict[str, str] = None):
        """Observe a value in a histogram"""
        labels = labels or {}
        key = f"{name}_{self._labels_to_string(labels)}"
        self.histograms[key].append(value)
        # Keep only last 1000 values
        if len(self.histograms[key]) > 1000:
            self.histograms[key] = self.histograms[key][-1000:]
        self._record_metric(name, MetricType.HISTOGRAM, value, labels)
    
    def record_timer(self, name: str, duration: float, labels: Dict[str, str] = None):
        """Record a timer metric"""
        labels = labels or {}
        key = f"{name}_{self._labels_to_string(labels)}"
        self.timers[key].append(duration)
        # Keep only last 1000 values
        if len(self.timers[key]) > 1000:
            self.timers[key] = self.timers[key][-1000:]
        self._record_metric(name, MetricType.TIMER, duration, labels)
    
    def record_summary(self, name: str, value: float, labels: Dict[str, str] = None):
        """Record a summary metric"""
        labels = labels or {}
        key = f"{name}_{self._labels_to_string(labels)}"
        self.summaries[key].append(value)
        # Keep only last 1000 values
        if len(self.summaries[key]) > 1000:
            self.summaries[key] = self.summaries[key][-1000:]
        self._record_metric(name, MetricType.SUMMARY, value, labels)
    
    def _record_metric(self, name: str, metric_type: MetricType, value: float, labels: Dict[str, str]):
        """Record metric in history and check alerts"""
        metric = MetricData(
            name=name,
            type=metric_type,
            value=value,
            timestamp=datetime.now(),
            labels=labels
        )
        
        self.metric_history[name].append(metric)
        self._check_alerts(metric)
    
    def _labels_to_string(self, labels: Dict[str, str]) -> str:
        """Convert labels dict to string for keying"""
        if not labels:
            return ""
        return "_".join(f"{k}_{v}" for k, v in sorted(labels.items()))
    
    def add_alert_rule(self, rule: AlertRule):
        """Add an alert rule"""
        self.alert_rules.append(rule)
        logging.info(f"Added alert rule: {rule.name}")
    
    def _check_alerts(self, metric: MetricData):
        """Check if metric triggers any alerts"""
        for rule in self.alert_rules:
            if not rule.enabled or rule.metric_name != metric.name:
                continue
            
            # Check cooldown
            last_alert = self.last_alert_times.get(rule.name)
            if last_alert and (datetime.now() - last_alert).total_seconds() < rule.cooldown_minutes * 60:
                continue
            
            # Evaluate condition
            triggered = False
            if rule.condition == ">" and metric.value > rule.threshold:
                triggered = True
            elif rule.condition == "<" and metric.value < rule.threshold:
                triggered = True
            elif rule.condition == ">=" and metric.value >= rule.threshold:
                triggered = True
            elif rule.condition == "<=" and metric.value <= rule.threshold:
                triggered = True
            elif rule.condition == "==" and metric.value == rule.threshold:
                triggered = True
            
            if triggered:
                self._trigger_alert(rule, metric)
    
    def _trigger_alert(self, rule: AlertRule, metric: MetricData):
        """Trigger an alert"""
        alert = {
            "rule_name": rule.name,
            "metric_name": metric.name,
            "metric_value": metric.value,
            "threshold": rule.threshold,
            "level": rule.level.value,
            "message": rule.message,
            "timestamp": datetime.now().isoformat(),
            "labels": metric.labels
        }
        
        self.alert_history.append(alert)
        self.last_alert_times[rule.name] = datetime.now()
        
        # Log alert
        log_level = {
            AlertLevel.INFO: logging.INFO,
            AlertLevel.WARNING: logging.WARNING,
            AlertLevel.ERROR: logging.ERROR,
            AlertLevel.CRITICAL: logging.CRITICAL
        }
        
        logging.log(
            log_level[rule.level],
            f"ALERT [{rule.level.value}] {rule.name}: {rule.message} "
            f"(Value: {metric.value}, Threshold: {rule.threshold})"
        )
    
    def get_metric_summary(self, name: str) -> Dict[str, Any]:
        """Get summary statistics for a metric"""
        if name not in self.metric_history:
            return {}
        
        history = list(self.metric_history[name])
        if not history:
            return {}
        
        values = [m.value for m in history]
        
        return {
            "name": name,
            "count": len(values),
            "min": min(values),
            "max": max(values),
            "mean": sum(values) / len(values),
            "latest": values[-1] if values else None,
            "timestamp": history[-1].timestamp.isoformat() if history else None
        }
    
    def get_all_metrics(self) -> Dict[str, Any]:
        """Get all current metrics"""
        return {
            "counters": dict(self.counters),
            "gauges": dict(self.gauges),
            "histograms": {k: {"count": len(v), "latest": v[-1] if v else None} for k, v in self.histograms.items()},
            "timers": {k: {"count": len(v), "latest": v[-1] if v else None} for k, v in self.timers.items()},
            "summaries": {k: {"count": len(v), "latest": v[-1] if v else None} for k, v in self.summaries.items()}
        }
    
    def export_metrics(self, format_type: str = "json") -> str:
        """Export metrics in specified format"""
        if format_type == "json":
            return json.dumps(self.get_all_metrics(), indent=2, default=str)
        elif format_type == "prometheus":
            return self._export_prometheus_format()
        else:
            raise ValueError(f"Unsupported format: {format_type}")
    
    def _export_prometheus_format(self) -> str:
        """Export metrics in Prometheus format"""
        lines = []
        
        # Counters
        for name, value in self.counters.items():
            lines.append(f"# TYPE {name} counter")
            lines.append(f"{name} {value}")
        
        # Gauges
        for name, value in self.gauges.items():
            lines.append(f"# TYPE {name} gauge")
            lines.append(f"{name} {value}")
        
        return "\n".join(lines)


class PerformanceMonitor:
    """Performance monitoring for operations"""
    
    def __init__(self, metrics_collector: MetricsCollector):
        self.metrics = metrics_collector
        self.active_operations = {}
    
    def start_operation(self, operation_name: str, context: Dict[str, Any] = None) -> str:
        """Start monitoring an operation"""
        operation_id = f"{operation_name}_{int(time.time() * 1000)}"
        self.active_operations[operation_id] = {
            "name": operation_name,
            "start_time": time.time(),
            "context": context or {}
        }
        return operation_id
    
    def end_operation(self, operation_id: str, success: bool = True, error: str = None):
        """End monitoring an operation"""
        if operation_id not in self.active_operations:
            return
        
        operation = self.active_operations[operation_id]
        duration = time.time() - operation["start_time"]
        
        # Record metrics
        labels = {
            "operation": operation["name"],
            "success": str(success).lower()
        }
        
        self.metrics.record_timer("operation_duration_seconds", duration, labels)
        self.metrics.increment_counter("operation_total", 1, labels)
        
        if not success:
            self.metrics.increment_counter("operation_errors_total", 1, {"operation": operation["name"]})
            if error:
                self.metrics.increment_counter("operation_errors_by_type", 1, {
                    "operation": operation["name"],
                    "error_type": error
                })
        
        # Clean up
        del self.active_operations[operation_id]
    
    def get_operation_stats(self, operation_name: str) -> Dict[str, Any]:
        """Get statistics for an operation"""
        # This would typically query the metrics collector
        # For now, return a placeholder
        return {
            "operation": operation_name,
            "total_calls": 0,
            "success_rate": 0.0,
            "avg_duration": 0.0,
            "error_count": 0
        }


class OperationTimer:
    """Context manager for timing operations"""
    
    def __init__(self, monitor: PerformanceMonitor, operation_name: str, context: Dict[str, Any] = None):
        self.monitor = monitor
        self.operation_name = operation_name
        self.context = context
        self.operation_id = None
        self.success = True
        self.error = None
    
    def __enter__(self):
        self.operation_id = self.monitor.start_operation(self.operation_name, self.context)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.success = False
            self.error = exc_type.__name__
        
        self.monitor.end_operation(self.operation_id, self.success, self.error)
        return False  # Don't suppress exceptions


class DocumentProcessingMetrics:
    """Specialized metrics for document processing pipeline"""
    
    def __init__(self, metrics_collector: MetricsCollector):
        self.metrics = metrics_collector
        self._setup_alert_rules()
    
    def _setup_alert_rules(self):
        """Setup default alert rules for document processing"""
        rules = [
            AlertRule(
                name="high_error_rate",
                metric_name="pipeline_errors_total",
                condition=">",
                threshold=10,
                level=AlertLevel.WARNING,
                message="High error rate in document processing pipeline"
            ),
            AlertRule(
                name="slow_processing",
                metric_name="document_processing_duration_seconds",
                condition=">",
                threshold=300,  # 5 minutes
                level=AlertLevel.WARNING,
                message="Document processing taking too long"
            ),
            AlertRule(
                name="low_classification_confidence",
                metric_name="classification_confidence_score",
                condition="<",
                threshold=0.7,
                level=AlertLevel.INFO,
                message="Low classification confidence detected"
            ),
            AlertRule(
                name="extraction_quality_poor",
                metric_name="extraction_quality_score",
                condition="<",
                threshold=0.6,
                level=AlertLevel.WARNING,
                message="Poor extraction quality detected"
            )
        ]
        
        for rule in rules:
            self.metrics.add_alert_rule(rule)
    
    def record_document_processed(self, filename: str, document_type: str, success: bool, duration: float):
        """Record document processing metrics"""
        labels = {
            "document_type": document_type,
            "success": str(success).lower()
        }
        
        self.metrics.increment_counter("documents_processed_total", 1, labels)
        self.metrics.record_timer("document_processing_duration_seconds", duration, labels)
        
        if not success:
            self.metrics.increment_counter("pipeline_errors_total", 1, {"document_type": document_type})
    
    def record_classification_result(self, document_type: str, confidence: float, provider: str):
        """Record classification metrics"""
        labels = {
            "document_type": document_type,
            "provider": provider
        }
        
        self.metrics.increment_counter("classifications_total", 1, labels)
        self.metrics.observe_histogram("classification_confidence_score", confidence, labels)
    
    def record_extraction_result(self, document_type: str, quality_score: float, extraction_time: float):
        """Record extraction metrics"""
        labels = {"document_type": document_type}
        
        self.metrics.increment_counter("extractions_total", 1, labels)
        self.metrics.observe_histogram("extraction_quality_score", quality_score, labels)
        self.metrics.record_timer("extraction_duration_seconds", extraction_time, labels)
    
    def record_api_call(self, provider: str, model: str, tokens_used: int, duration: float, success: bool):
        """Record API call metrics"""
        labels = {
            "provider": provider,
            "model": model,
            "success": str(success).lower()
        }
        
        self.metrics.increment_counter("api_calls_total", 1, labels)
        self.metrics.increment_counter("api_tokens_used_total", tokens_used, labels)
        self.metrics.record_timer("api_call_duration_seconds", duration, labels)
        
        if not success:
            self.metrics.increment_counter("api_errors_total", 1, {"provider": provider, "model": model})
    
    def get_processing_summary(self) -> Dict[str, Any]:
        """Get summary of document processing metrics"""
        return {
            "documents_processed": self.metrics.get_metric_summary("documents_processed_total"),
            "classification_confidence": self.metrics.get_metric_summary("classification_confidence_score"),
            "extraction_quality": self.metrics.get_metric_summary("extraction_quality_score"),
            "processing_duration": self.metrics.get_metric_summary("document_processing_duration_seconds"),
            "error_rate": self.metrics.get_metric_summary("pipeline_errors_total")
        }


# Global metrics instance
_global_metrics = None
_global_performance_monitor = None
_global_doc_metrics = None


def get_metrics_collector() -> MetricsCollector:
    """Get global metrics collector instance"""
    global _global_metrics
    if _global_metrics is None:
        _global_metrics = MetricsCollector()
    return _global_metrics


def get_performance_monitor() -> PerformanceMonitor:
    """Get global performance monitor instance"""
    global _global_performance_monitor
    if _global_performance_monitor is None:
        _global_performance_monitor = PerformanceMonitor(get_metrics_collector())
    return _global_performance_monitor


def get_document_metrics() -> DocumentProcessingMetrics:
    """Get global document processing metrics instance"""
    global _global_doc_metrics
    if _global_doc_metrics is None:
        _global_doc_metrics = DocumentProcessingMetrics(get_metrics_collector())
    return _global_doc_metrics


def monitor_operation(operation_name: str, context: Dict[str, Any] = None):
    """Decorator to monitor operation performance"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            with OperationTimer(get_performance_monitor(), operation_name, context):
                return func(*args, **kwargs)
        return wrapper
    return decorator


def export_metrics_to_file(filepath: str, format_type: str = "json"):
    """Export metrics to file"""
    metrics = get_metrics_collector()
    content = metrics.export_metrics(format_type)
    
    with open(filepath, 'w') as f:
        f.write(content)
    
    logging.info(f"Metrics exported to {filepath}")


# Example usage and setup
def setup_monitoring():
    """Setup monitoring with default configuration"""
    metrics = get_metrics_collector()
    doc_metrics = get_document_metrics()
    
    logging.info("Monitoring system initialized")
    return metrics, doc_metrics 