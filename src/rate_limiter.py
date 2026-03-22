from collections import defaultdict
import time
from functools import wraps

class RateLimiter:
    """Token bucket rate limiter for API endpoints."""
    
    def __init__(self, max_requests: int = 100, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._requests = defaultdict(list)

    def is_allowed(self, client_id: str) -> bool:
        now = time.time()
        window_start = now - self.window_seconds
        
        # Clean old entries
        self._requests[client_id] = [
            t for t in self._requests[client_id] if t > window_start
        ]
        
        if len(self._requests[client_id]) >= self.max_requests:
            return False
        
        self._requests[client_id].append(now)
        return True

    def get_remaining(self, client_id: str) -> int:
        now = time.time()
        window_start = now - self.window_seconds
        recent = [t for t in self._requests[client_id] if t > window_start]
        return max(0, self.max_requests - len(recent))

    def rate_limit(self, client_id_func):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                client_id = client_id_func(*args, **kwargs)
                if not self.is_allowed(client_id):
                    raise Exception(f"Rate limit exceeded for {client_id}")
                return func(*args, **kwargs)
            return wrapper
        return decorator
