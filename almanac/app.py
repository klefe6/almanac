"""
Main Dash Application

Entry point for the Almanac Futures application with caching and modular structure.
"""

import dash
from dash import dcc, html
from flask_caching import Cache
from .config import get_config
import os
import json
from datetime import datetime
import threading
import time
from functools import wraps

# Initialize Dash app
app = dash.Dash(
    __name__,
    suppress_callback_exceptions=True,
    title="Almanac Futures - Intraday Analysis"
)

# Add health check endpoint
@app.server.route('/status')
def health_check():
    """
    Health check endpoint that returns application status.
    """
    return json.dumps({
        'status': 'healthy',
        'service': 'Almanac Futures',
        'timestamp': datetime.now().isoformat(),
        'version': '1.0.0',
        'uptime': 'running'
    }, indent=2)

# Cache monitoring system
class CacheMonitor:
    """Simple cache hit/miss tracking system."""
    
    def __init__(self):
        self.hits = 0
        self.misses = 0
        self.total_requests = 0
        self.start_time = time.time()
        self.lock = threading.Lock()
    
    def record_hit(self):
        """Record a cache hit."""
        with self.lock:
            self.hits += 1
            self.total_requests += 1
    
    def record_miss(self):
        """Record a cache miss."""
        with self.lock:
            self.misses += 1
            self.total_requests += 1
    
    def get_stats(self):
        """Get current cache statistics."""
        with self.lock:
            uptime = time.time() - self.start_time
            hit_rate = (self.hits / self.total_requests * 100) if self.total_requests > 0 else 0
            
            return {
                'hits': self.hits,
                'misses': self.misses,
                'total_requests': self.total_requests,
                'hit_rate_percent': round(hit_rate, 2),
                'uptime_seconds': round(uptime, 2),
                'requests_per_minute': round(self.total_requests / (uptime / 60), 2) if uptime > 0 else 0
            }

# Global cache monitor instance
cache_monitor = CacheMonitor()

def monitored_memoize(timeout=None):
    """
    Decorator that wraps cache.memoize with hit/miss tracking.
    """
    def decorator(func):
        # Get the original memoize decorator
        memoized_func = cache.memoize(timeout=timeout)(func)
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Generate cache key (simplified)
            cache_key = f"{func.__name__}_{hash(str(args) + str(sorted(kwargs.items())))}"
            
            # Check if result is in cache
            try:
                result = cache.get(cache_key)
                if result is not None:
                    cache_monitor.record_hit()
                    return result
                else:
                    cache_monitor.record_miss()
                    # Execute function and cache result
                    result = func(*args, **kwargs)
                    cache.set(cache_key, result, timeout=timeout)
                    return result
            except Exception:
                # If cache fails, just execute function
                cache_monitor.record_miss()
                return func(*args, **kwargs)
        
        return wrapper
    return decorator

# Add cache stats endpoint
@app.server.route('/cache-stats')
def cache_stats():
    """
    Cache statistics endpoint that returns hit/miss information.
    """
    stats = cache_monitor.get_stats()
    stats['timestamp'] = datetime.now().isoformat()
    stats['cache_type'] = 'filesystem'
    stats['cache_timeout'] = 300  # 5 minutes
    
    return json.dumps(stats, indent=2)

# Metrics endpoint and lightweight dashboard
from .utils.monitoring import performance_monitor, health_checker

@app.server.route('/metrics')
def metrics():
    summary = performance_monitor.get_performance_summary()
    health = health_checker.get_health_status()
    payload = {
        'performance': summary,
        'health': health,
        'cache': cache_monitor.get_stats(),
        'timestamp': datetime.now().isoformat()
    }
    return json.dumps(payload, indent=2)

@app.server.route('/metrics-dashboard')
def metrics_dashboard():
    # Simple HTML page that polls /metrics and /cache-stats
    html_page = f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset=\"utf-8\" />
  <title>Almanac Metrics Dashboard</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 20px; }}
    .grid {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px; }}
    .card {{ border: 1px solid #ddd; border-radius: 8px; padding: 12px; }}
    pre {{ background: #f7f7f7; padding: 12px; border-radius: 6px; overflow-x: auto; }}
    .title {{ font-weight: bold; margin-bottom: 8px; }}
  </style>
  <script>
    async function fetchJson(url) {{
      const res = await fetch(url);
      return res.json();
    }}
    async function refresh() {{
      try {{
        const metrics = await fetchJson('/metrics');
        const cache = await fetch('/cache-stats').then(r => r.text());
        document.getElementById('perf').textContent = JSON.stringify(metrics.performance, null, 2);
        document.getElementById('health').textContent = JSON.stringify(metrics.health, null, 2);
        document.getElementById('cache').textContent = cache;
        document.getElementById('ts').textContent = metrics.timestamp;
      }} catch (e) {{
        document.getElementById('health').textContent = 'Error loading metrics: ' + e;
      }}
    }}
    setInterval(refresh, 3000);
    window.onload = refresh;
  </script>
  </head>
  <body>
    <h2>Almanac Metrics Dashboard</h2>
    <div>Last Updated: <span id=\"ts\">-</span></div>
    <div class=\"grid\">
      <div class=\"card\"><div class=\"title\">Performance Summary</div><pre id=\"perf\">Loading...</pre></div>
      <div class=\"card\"><div class=\"title\">Health Status</div><pre id=\"health\">Loading...</pre></div>
      <div class=\"card\"><div class=\"title\">Cache Stats</div><pre id=\"cache\">Loading...</pre></div>
    </div>
  </body>
</html>
"""
    return html_page

# Configure Flask-Caching via centralized config
_cfg = get_config()
cache_config = _cfg.to_cache_config()

cache = Cache()
cache.init_app(app.server, config=cache_config)

# Create a monitored cache wrapper
def get_monitored_cache():
    """
    Return a cache object with monitoring capabilities.
    """
    class MonitoredCache:
        def __init__(self, cache_instance, monitor):
            self.cache = cache_instance
            self.monitor = monitor
        
        def memoize(self, timeout=None):
            """Return a monitored memoize decorator."""
            return monitored_memoize(timeout=timeout)
        
        def get(self, key):
            """Get value from cache and record hit/miss."""
            result = self.cache.get(key)
            if result is not None:
                self.monitor.record_hit()
            else:
                self.monitor.record_miss()
            return result
        
        def set(self, key, value, timeout=None):
            """Set value in cache."""
            return self.cache.set(key, value, timeout=timeout)
        
        def delete(self, key):
            """Delete value from cache."""
            return self.cache.delete(key)
        
        def clear(self):
            """Clear all cache."""
            return self.cache.clear()
    
    return MonitoredCache(cache, cache_monitor)

# Create monitored cache instance
monitored_cache = get_monitored_cache()

# Flag to track if app has been initialized
_app_initialized = False


def _initialize_app():
    """Initialize the Dash app layout and callbacks."""
    global _app_initialized
    if _app_initialized:
        return
    
    try:
        # Import and register page callbacks
        from .pages.profile import create_profile_layout, register_profile_callbacks
        
        # Create layout
        app.layout = create_profile_layout()
        
        # Register callbacks with monitored cache
        register_profile_callbacks(app, monitored_cache)
        
        _app_initialized = True
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Failed to initialize app: {e}", exc_info=True)
        raise


def run_server(host='127.0.0.1', port=8085, debug=True):
    """
    Run the Dash application server.
    
    Args:
        host: Host address
        port: Port number
        debug: Whether to run in debug mode
    """
    # Initialize app before running (lazy initialization)
    _initialize_app()
    app.run(host=host, port=port, debug=debug)


if __name__ == '__main__':
    run_server()

