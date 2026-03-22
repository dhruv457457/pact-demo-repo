import asyncio
from contextlib import asynccontextmanager

class ConnectionPool:
    """Fixed connection pool with proper cleanup and retry logic."""
    
    def __init__(self, dsn: str, min_size: int = 5, max_size: int = 20):
        self.dsn = dsn
        self.min_size = min_size
        self.max_size = max_size
        self._pool = []
        self._in_use = set()
        self._lock = asyncio.Lock()

    async def initialize(self):
        for _ in range(self.min_size):
            conn = await self._create_connection()
            self._pool.append(conn)

    async def _create_connection(self):
        # Simulate connection creation with retry
        for attempt in range(3):
            try:
                return {"dsn": self.dsn, "active": True, "id": id(object())}
            except Exception:
                if attempt == 2:
                    raise
                await asyncio.sleep(0.1 * (attempt + 1))

    @asynccontextmanager
    async def acquire(self):
        conn = None
        async with self._lock:
            if self._pool:
                conn = self._pool.pop()
            elif len(self._in_use) < self.max_size:
                conn = await self._create_connection()
            
        if conn is None:
            raise Exception("Connection pool exhausted")
            
        self._in_use.add(conn["id"])
        try:
            yield conn
        finally:
            async with self._lock:
                self._in_use.discard(conn["id"])
                if conn["active"]:
                    self._pool.append(conn)

    async def close(self):
        async with self._lock:
            self._pool.clear()
            self._in_use.clear()
