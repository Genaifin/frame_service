import hashlib
import json
import time
from typing import Dict, List, Optional, Any
from pathlib import Path
import os

class QueryCache:
    """
    Simple in-memory and file-based cache for SQL query results
    Helps avoid re-executing expensive queries
    """
    
    def __init__(self, cache_dir: str = "data/query_cache", ttl_seconds: int = 3600):
        """
        Initialize query cache
        
        Args:
            cache_dir: Directory to store cache files
            ttl_seconds: Time to live for cache entries (default 1 hour)
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.ttl_seconds = ttl_seconds
        self.memory_cache: Dict[str, Dict[str, Any]] = {}
        self._cleanup_old_cache()
    
    def _get_cache_key(self, sql_query: str) -> str:
        """Generate a unique cache key from SQL query"""
        # Normalize query by removing extra whitespace and converting to lowercase
        normalized = " ".join(sql_query.lower().split())
        return hashlib.md5(normalized.encode()).hexdigest()
    
    def _get_cache_file(self, cache_key: str) -> Path:
        """Get cache file path for a cache key"""
        return self.cache_dir / f"{cache_key}.json"
    
    def _cleanup_old_cache(self):
        """Remove expired cache files"""
        try:
            current_time = time.time()
            for cache_file in self.cache_dir.glob("*.json"):
                if cache_file.stat().st_mtime < (current_time - self.ttl_seconds):
                    cache_file.unlink()
        except Exception:
            pass
    
    def get(self, sql_query: str) -> Optional[List[Dict]]:
        """
        Get cached results for a SQL query
        
        Args:
            sql_query: The SQL query to look up
            
        Returns:
            Cached results if found and not expired, None otherwise
        """
        cache_key = self._get_cache_key(sql_query)
        
        # Check memory cache first
        if cache_key in self.memory_cache:
            entry = self.memory_cache[cache_key]
            if time.time() - entry["timestamp"] < self.ttl_seconds:
                return entry["results"]
            else:
                # Expired, remove from memory
                del self.memory_cache[cache_key]
        
        # Check file cache
        cache_file = self._get_cache_file(cache_key)
        if cache_file.exists():
            try:
                with open(cache_file, "r") as f:
                    entry = json.load(f)
                
                if time.time() - entry["timestamp"] < self.ttl_seconds:
                    # Load into memory cache for faster access
                    self.memory_cache[cache_key] = entry
                    return entry["results"]
                else:
                    # Expired, remove file
                    cache_file.unlink()
            except Exception:
                # Corrupted cache file, remove it
                cache_file.unlink()
        
        return None
    
    def set(self, sql_query: str, results: List[Dict]):
        """
        Cache query results
        
        Args:
            sql_query: The SQL query
            results: The query results to cache
        """
        cache_key = self._get_cache_key(sql_query)
        
        entry = {
            "query": sql_query,
            "results": results,
            "timestamp": time.time()
        }
        
        # Store in memory cache
        self.memory_cache[cache_key] = entry
        
        # Store in file cache
        cache_file = self._get_cache_file(cache_key)
        try:
            with open(cache_file, "w") as f:
                json.dump(entry, f)
        except Exception:
            # If file write fails, just continue with memory cache
            pass
    
    def invalidate_all(self):
        """Clear all cache entries"""
        self.memory_cache.clear()
        try:
            for cache_file in self.cache_dir.glob("*.json"):
                cache_file.unlink()
        except Exception:
            pass
    
    def get_cache_stats(self) -> Dict[str, int]:
        """Get cache statistics"""
        return {
            "memory_entries": len(self.memory_cache),
            "file_entries": len(list(self.cache_dir.glob("*.json"))),
            "ttl_seconds": self.ttl_seconds
        }


# Global cache instance
_query_cache: Optional[QueryCache] = None

def get_query_cache() -> QueryCache:
    """Get or create the global query cache instance"""
    global _query_cache
    if _query_cache is None:
        # Use environment variable for TTL if available
        ttl = int(os.getenv("QUERY_CACHE_TTL", "3600"))
        _query_cache = QueryCache(ttl_seconds=ttl)
    return _query_cache
