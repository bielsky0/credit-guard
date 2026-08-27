# Python Internals & Server Architecture — Learning Guide

*Comprehensive guide for Strong Mid/Senior Python Developer interviews and daily work.*

---

## Part 1: How Python Works Under the Hood

### 1.1 From Source Code to Execution

```python
# 1. Source code (.py)
def add(a, b):
    return a + b

# 2. Compilation to bytecode (.pyc in __pycache__/)
# python -m py_compile main.py
# or import main (automatic)

# 3. Bytecode = instructions for Python Virtual Machine (PVM)
import dis
dis.dis(add)
```

**Output:**
```
  2           0 LOAD_FAST                0 (a)
              2 LOAD_FAST                1 (b)
              4 BINARY_OP                0 (+)
              6 RETURN_VALUE
```

### 1.2 Key Components

| Component | Role |
|-----------|------|
| **Parser** | Source code → AST (Abstract Syntax Tree) |
| **Compiler** | AST → Bytecode (`.pyc`) |
| **PVM (Python Virtual Machine)** | Bytecode interpreter — stack machine |
| **Memory Manager** | Refcounting + GC (generational) |
| **GIL (Global Interpreter Lock)** | Mutex protecting internal structures |

### 1.3 GIL — Practical Consequences

```python
import threading
import time

counter = 0

def increment():
    global counter
    for _ in range(1_000_000):
        counter += 1  # NOT atomic in Python!

t1 = threading.Thread(target=increment)
t2 = threading.Thread(target=increment)
t1.start(); t2.start()
t1.join(); t2.join()
print(counter)  # ~1.3M instead of 2M — race condition + GIL contention
```

**Solutions:**
- `multiprocessing` — separate processes = separate GILs
- `asyncio` — for I/O-bound (GIL released at `await`)
- `ProcessPoolExecutor` — for CPU-bound
- `threading` — only for I/O-bound (GIL released during syscalls)

---

## Part 2: Networking & Server Fundamentals

### 2.1 Raw Socket — The Foundation

```python
# server_raw.py
import socket

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind(('0.0.0.0', 8000))
server.listen(5)

print("Listening on port 8000...")

while True:
    client, addr = server.accept()  # BLOCKING
    request = client.recv(1024).decode()
    print(f"Request:\n{request}")
    
    response = b"HTTP/1.1 200 OK\r\nContent-Length: 13\r\n\r\nHello, World!"
    client.send(response)
    client.close()
```

```bash
# Terminal 1
python server_raw.py

# Terminal 2
curl http://localhost:8000
```

### 2.2 The Problem: One Request at a Time

```
Client 1 ──────────► [accept] ────► [recv] ────► [send] ────► [close]
Client 2 ────────────────── WAIT ─────────────────────────────────►
Client 3 ──────────────────────────── WAIT ────────────────────────►
```

---

## Part 3: Concurrency Models in Python

### 3.1 Threading (Preemptive, GIL-Limited)

```python
# server_threaded.py
import socket
import threading

def handle_client(client, addr):
    request = client.recv(1024).decode()
    response = b"HTTP/1.1 200 OK\r\nContent-Length: 13\r\n\r\nHello, World!"
    client.send(response)
    client.close()

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind(('0.0.0.0', 8000))
server.listen(5)

while True:
    client, addr = server.accept()
    t = threading.Thread(target=handle_client, args=(client, addr))
    t.start()
```

| Pros | Cons |
|------|------|
| Simple API | GIL = 1 thread executes bytecode at a time |
| Shared memory | Context switching overhead |
| Good for I/O-bound | Limit ~100-1000 threads |

---

### 3.2 Multiprocessing (True Parallelism)

```python
# server_multiprocess.py
import socket
from multiprocessing import Process

def handle_client(client, addr):
    request = client.recv(1024).decode()
    response = b"HTTP/1.1 200 OK\r\nContent-Length: 13\r\n\r\nHello, World!"
    client.send(response)
    client.close()

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind(('0.0.0.0', 8000))
server.listen(5)

while True:
    client, addr = server.accept()
    p = Process(target=handle_client, args=(client, addr))
    p.start()
    client.close()  # Parent closes its fd (child has copy)
```

| Pros | Cons |
|------|------|
| True parallelism (multiple CPU) | Heavy memory (copy-on-write) |
| Each process = own GIL | IPC complicated |
| Crash isolation | Startup time |

---

### 3.3 Async/Await (Cooperative, Single-Threaded)

```python
# server_async.py
import asyncio

async def handle_client(reader, writer):
    request = await reader.read(1024)
    response = b"HTTP/1.1 200 OK\r\nContent-Length: 13\r\n\r\nHello, World!"
    writer.write(response)
    await writer.drain()
    writer.close()

async def main():
    server = await asyncio.start_server(handle_client, '0.0.0.0', 8000)
    async with server:
        await server.serve_forever()

asyncio.run(main())
```

**How It Works:**
```
Event Loop (1 thread):
┌─────────────────────────────────────────────┐
│ Task 1: await reader.read()  ──► YIELD     │
│ Task 2: await reader.read()  ──► YIELD     │
│ Task 3: await writer.drain() ──► YIELD     │
│ ...                                         │
│ When I/O ready → resume task               │
└─────────────────────────────────────────────┘
```

---

## Part 4: WSGI vs ASGI — The Standards

### 4.1 WSGI (Web Server Gateway Interface) — Sync

```python
# wsgi_app.py
def application(environ, start_response):
    """environ = dict with request, start_response = callback for headers"""
    status = '200 OK'
    headers = [('Content-Type', 'text/plain')]
    start_response(status, headers)
    return [b'Hello, WSGI!']

# Run: gunicorn wsgi_app:application -w 4
```

**Characteristics:**
- 1 request = 1 thread/worker (blocking)
- `environ` = dict (CGI-style)
- Response = iterable (list of bytes)
- **Django, Flask, Pyramid** — native WSGI

---

### 4.2 ASGI (Asynchronous Server Gateway Interface) — Async

```python
# asgi_app.py
async def application(scope, receive, send):
    """scope = dict (type, path, headers), receive/send = async callables"""
    assert scope['type'] == 'http'
    
    # Receive request body
    body = b''
    while True:
        message = await receive()
        body += message.get('body', b'')
        if not message.get('more_body', False):
            break
    
    # Send response
    await send({
        'type': 'http.response.start',
        'status': 200,
        'headers': [[b'content-type', b'text/plain']],
    })
    await send({
        'type': 'http.response.body',
        'body': b'Hello, ASGI!',
    })

# Run: uvicorn asgi_app:application
```

**Characteristics:**
- 1 event loop = thousands of requests
- `receive`/`send` = async primitives
- **FastAPI, Starlette, Quart, Django Channels** — native ASGI

---

### 4.3 Architecture Comparison

```
WSGI (Gunicorn + sync workers):
┌─────────────────────────────────────┐
│ Gunicorn Master                     │
├── Worker 1 (PID) ── Thread 1 ──────►│ 1 request
├── Worker 2 (PID) ── Thread 1 ──────►│ 1 request
├── Worker 3 (PID) ── Thread 1 ──────►│ 1 request
└── Worker 4 (PID) ── Thread 1 ──────►│ 1 request

ASGI (Gunicorn + UvicornWorker):
┌─────────────────────────────────────┐
│ Gunicorn Master                     │
├── Worker 1 (PID) ── Event Loop ────►│ 1000+ concurrent
├── Worker 2 (PID) ── Event Loop ────►│ 1000+ concurrent
├── Worker 3 (PID) ── Event Loop ────►│ 1000+ concurrent
└── Worker 4 (PID) ── Event Loop ────►│ 1000+ concurrent
```

---

## Part 5: Production — Gunicorn + Uvicorn

### 5.1 Why Gunicorn Over Raw Uvicorn?

```python
# Uvicorn alone (dev):
uvicorn app:app --reload --workers 4
# Problem: no graceful reload, no process management

# Gunicorn + UvicornWorker (prod):
gunicorn -k uvicorn.workers.UvicornWorker -w 4 app:app
# Benefits:
# - Master process manages workers
# - Graceful reload (SIGHUP)
# - Worker recycling (--max-requests)
# - Preload app (copy-on-write memory)
# - Health checks, logging, signals
```

### 5.2 Production Configuration

```python
# gunicorn.conf.py
import os
import multiprocessing

# Workers: I/O-bound = 2 * CPU cores
workers = int(os.getenv("WEB_CONCURRENCY", multiprocessing.cpu_count() * 2))
worker_class = "uvicorn.workers.UvicornWorker"
worker_connections = 1000  # async connections per worker

# Memory optimization
worker_tmp_dir = "/dev/shm"  # RAM disk for heartbeat
preload_app = True  # Load app before fork (COW memory)

# Stability
max_requests = 5000
max_requests_jitter = 1000
timeout = 30
graceful_timeout = 10

# Logging
accesslog = "-"
errorlog = "-"
loglevel = "info"

# Post-fork initialization (DB pools, Kafka producers)
def post_fork(server, worker):
    # Initialize connections HERE (after fork)
    pass
```

### 5.3 Production Dockerfile

```dockerfile
FROM python:3.12-slim

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev curl && \
    rm -rf /var/lib/apt/lists/*

# Python deps (cached layer)
COPY pyproject.toml ./
RUN pip install --no-cache-dir -e .

# App code
COPY src ./src
COPY gunicorn.conf.py ./

# Non-root user
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

# Healthcheck
HEALTHCHECK --interval=10s --timeout=5s --start-period=20s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

CMD ["gunicorn", "-c", "gunicorn.conf.py", "app:app"]
```

---

## Part 6: Hands-On Exercises

### Exercise 1: Inspect Bytecode
```bash
python -m dis your_file.py
# Or in REPL:
import dis; dis.dis(your_function)
```

### Exercise 2: Benchmark Concurrency Models
```python
# benchmark.py
import asyncio
import threading
import multiprocessing
import time

def sync_io():
    time.sleep(0.1)  # Simulate I/O

async def async_io():
    await asyncio.sleep(0.1)

def cpu_heavy():
    sum(i*i for i in range(100_000))

# Test 1: 100 sync I/O calls
# Test 2: 100 threaded I/O calls  
# Test 3: 100 async I/O calls
# Test 4: 4 CPU-heavy in threading vs multiprocessing vs ProcessPoolExecutor
```

### Exercise 3: Build Servers
1. Raw socket (blocking)
2. Threading server
3. Multiprocessing server
4. Asyncio server
5. WSGI app + Gunicorn
6. ASGI app + Uvicorn + Gunicorn

### Exercise 4: See GIL in Action
```python
import threading, time

def cpu_task():
    sum(i*i for i in range(5_000_000))

# 1 thread
start = time.time(); cpu_task(); print(f"1 thread: {time.time()-start:.2f}s")

# 2 threads (GIL contention!)
t1 = threading.Thread(target=cpu_task)
t2 = threading.Thread(target=cpu_task)
start = time.time(); t1.start(); t2.start(); t1.join(); t2.join()
print(f"2 threads: {time.time()-start:.2f}s")  # SLOWER!
```

### Exercise 5: Memory Profiling
```bash
pip install memory_profiler
python -m memory_profiler your_script.py
```

---

## Concept Map to Memorize

```
┌─────────────────────────────────────────────────────────────┐
│                    PYTHON RUNTIME                            │
├─────────────────────────────────────────────────────────────┤
│  Source (.py) ──► AST ──► Bytecode (.pyc) ──► PVM           │
│                                    │                         │
│                                    ▼                         │
│                          ┌─────────────────┐                 │
│                          │   GIL (Mutex)   │                 │
│                          └────────┬────────┘                 │
│                                   │                           │
│              ┌────────────────────┼────────────────────┐     │
│              ▼                    ▼                    ▼     │
│       ┌───────────┐        ┌───────────┐          ┌────────┐│
│       │ Threading │        │Multiprocess│         │ Asyncio││
│       │ (1 GIL)   │        │ (N × GIL) │         │(1 GIL, ││
│       │ I/O-bound │        │ CPU-bound │         │ yield) ││
│       └───────────┘        └───────────┘          └────────┘│
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    SERVER STACK                              │
├─────────────────────────────────────────────────────────────┤
│  Raw Socket → WSGI (sync) / ASGI (async)                    │
│       │                    │                                 │
│       ▼                    ▼                                 │
│  Gunicorn              Uvicorn                               │
│  (process mgr)         (ASGI server)                         │
│       │                    │                                 │
│       └────────┬─────────┘                                 │
│                ▼                                           │
│      Gunicorn + UvicornWorker (PROD)                       │
└─────────────────────────────────────────────────────────────┘
```

---

## Part 7: Django Internals — Sync WSGI Stack

### 7.1 Request/Response Cycle (WSGI)

```
Browser Request
      │
      ▼
┌─────────────────────────────────────────────────────────────┐
│ GUNICORN MASTER (prefork workers)                           │
│   Worker 1 (PID) ── sync worker ──► Django WSGI App         │
└─────────────────────────────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────────────────────────────┐
│ DJANGO CORE (django/core/handlers/wsgi.py)                  │
│                                                              │
│  1. WSGIHandler.__call__(environ, start_response)           │
│     │                                                        │
│     ▼                                                        │
│  2. Request Factory ──► HttpRequest object                  │
│     │       (environ → request.META, request.GET, etc.)     │
│     ▼                                                        │
│  3. MIDDLEWARE STACK (request phase)                        │
│     │  ┌─────────────────────────────────────────────────┐  │
│     │  │ SecurityMiddleware                              │  │
│     │  │ SessionMiddleware  (request.session)            │  │
│     │  │ CommonMiddleware                               │  │
│     │  │ CsrfViewMiddleware                             │  │
│     │  │ AuthenticationMiddleware (request.user)         │  │
│     │  │ MessageMiddleware                              │  │
│     │  │ ... custom middleware                           │  │
│     │  └─────────────────────────────────────────────────┘  │
│     ▼                                                        │
│  4. URL RESOLVER (django/urls/resolvers.py)                 │
│     │  URL patterns → ResolverMatch(func, args, kwargs)     │
│     ▼                                                        │
│  5. VIEW EXECUTION                                           │
│     │  FBV: view_func(request, *args, **kwargs)             │
│     │  CBV: View.as_view()(request, *args, **kwargs)        │
│     ▼                                                        │
│  6. MIDDLEWARE STACK (response phase) — REVERSE ORDER       │
│     ▼                                                        │
│  7. HttpResponse → WSGI iterable → start_response()         │
└─────────────────────────────────────────────────────────────┘
```

### 7.2 Middleware — Hook Points

```python
# middleware.py
class CustomMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response  # Next middleware or view
    
    def __call__(self, request):
        # PRE-VIEW (request phase)
        print("Before view")
        
        response = self.get_response(request)  # CALL NEXT
        
        # POST-VIEW (response phase)
        print("After view")
        return response
    
    # Optional: process_view, process_exception, process_template_response
    def process_view(self, request, view_func, view_args, view_kwargs):
        return None  # or HttpResponse to short-circuit
    
    def process_exception(self, request, exception):
        return None  # or HttpResponse for custom error handling
```

**Middleware Order = Critical** (settings.MIDDLEWARE list order):
- Request: TOP → BOTTOM
- Response: BOTTOM → TOP

---

### 7.3 ORM — Query Execution

```python
# models.py
class Author(models.Model):
    name = models.CharField(max_length=100)

class Book(models.Model):
    title = models.CharField(max_length=200)
    author = models.ForeignKey(Author, on_delete=models.CASCADE)
    published = models.DateField()

# Query
books = Book.objects.select_related('author').filter(published__year=2024)
```

**What Happens:**

```
Book.objects.filter(...) 
    │
    ▼
QuerySet (lazy) ──► Does NOT execute SQL!
    │
    ▼ (evaluation: list(), for loop, len(), bool())
SQLCompiler (django/db/models/sql/compiler.py)
    │
    ▼
SQL: SELECT "book"."id", "book"."title", "book"."author_id", 
            "author"."id", "author"."name"
     FROM "book" 
     INNER JOIN "author" ON ("book"."author_id" = "author"."id")
     WHERE "book"."published" BETWEEN '2024-01-01' AND '2024-12-31'
    │
    ▼
Cursor.execute(sql, params)  ──► psycopg2 / asyncpg
    │
    ▼
Row → Model Instance (Model.__init__ + field cleaning)
```

**N+1 Problem:**
```python
# BAD - N+1 queries
books = Book.objects.all()  # 1 query
for book in books:
    print(book.author.name)  # N queries!

# GOOD - 1 query with JOIN
books = Book.objects.select_related('author').all()
for book in books:
    print(book.author.name)  # 0 additional queries
```

### 7.4 Django Async (4.1+)

```python
# views.py (async)
async def book_list_async(request):
    # Async ORM (new API)
    books = [b async for b in Book.objects.select_related('author').filter(published__year=2024)]
    return JsonResponse({"books": [b.title for b in books]})

# Requires ASGI server:
# gunicorn -k uvicorn.workers.UvicornWorker myproject.asgi:application
```

**Limitations of async Django:**
- Middleware sync by default (`SyncToAsyncMiddleware` wraps)
- Templates sync only
- Cache/Session backend must support async
- Third-party packages often sync-only

---

## Part 8: FastAPI Internals — ASGI + Starlette + Pydantic

### 8.1 Architecture Layers

```
┌─────────────────────────────────────────────────────────────┐
│                    FASTAPI APPLICATION                       │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ STARLETTE (ASGI Framework)                              │ │
│  │  • Routing (Router, Mount, Path)                        │ │
│  │  • Middleware (BaseHTTPMiddleware)                      │ │
│  │  • Request/Response classes                             │ │
│  │  • WebSocket, SSE, StaticFiles                          │ │
│  │  • TestClient                                           │ │
│  └────────────────────────────────────────────────────────┘ │
│                              │                               │
│                              ▼                               │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ FASTAPI LAYER                                           │ │
│  │  • Dependency Injection (Depends, Security)            │ │
│  │  • Parameter extraction (Path, Query, Header, Cookie)  │ │
│  │  • Body parsing (Pydantic models)                       │ │
│  │  • Response serialization + validation                  │ │
│  │  • OpenAPI schema generation                            │ │
│  │  • Automatic docs (/docs, /redoc)                       │ │
│  └────────────────────────────────────────────────────────┘ │
│                              │                               │
│                              ▼                               │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ PYDANTIC V2 (Validation & Serialization)               │ │
│  │  • Core: pydantic-core (Rust)                           │ │
│  │  • TypeAdapter, BaseModel, Field, Validators           │ │
│  │  • Serialization: model_dump(), model_dump_json()      │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### 8.2 Request Flow (ASGI)

```python
# FastAPI app = Starlette app = ASGI callable
app = FastAPI()

# This is the ASGI entry point:
async def app(scope, receive, send):
    # Starlette Router.match(scope) → route, path_params
    # Starlette Request(scope, receive)
    # Dependency Injection resolution
    # Endpoint execution
    # Response serialization
    # Starlette Response(scope, send)
    pass
```

**Detailed Flow:**

```
ASGI Scope (dict) ──► Starlette Router ──► Route Match
                              │
                              ▼
                     Request(scope, receive)
                              │
                              ▼
              ┌─────────────────────────────────────┐
              │ DEPENDENCY INJECTION RESOLUTION     │
              │  (fastapi/dependencies/utils.py)    │
              │                                      │
              │  1. Extract path/query/header/cookie │
              │  2. Parse Body → Pydantic model      │
              │  3. Resolve Depends() tree           │
              │     │                                │
              │     ▼                                │
              │  Dependency Cache (per request)      │
              │  - Singleton per request             │
              │  - yield = cleanup after response    │
              └─────────────────────────────────────┘
                              │
                              ▼
                     Endpoint Execution
                     (sync or async function)
                              │
                              ▼
              ┌─────────────────────────────────────┐
              │ RESPONSE PROCESSING                 │
              │                                      │
              │  1. Return value → Response Model    │
              │  2. Pydantic validation (response)   │
              │  3. Serialization (JSON, msgpack)    │
              │  4. BackgroundTasks execution        │
              └─────────────────────────────────────┘
```

### 8.3 Dependency Injection — How It Works

```python
# dependencies.py
from fastapi import Depends, Header, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

# 1. Simple dependency
async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with async_session() as session:
        yield session  # cleanup after request

# 2. Parameter extraction
async def get_current_user(
    authorization: str = Header(None),
    db: AsyncSession = Depends(get_db)
) -> User:
    token = authorization.replace("Bearer ", "")
    user = await verify_token(token, db)
    if not user:
        raise HTTPException(401)
    return user

# 3. Class-based dependency (stateful)
class Pagination:
    def __init__(self, max_limit: int = 100):
        self.max_limit = max_limit
    
    def __call__(self, limit: int = 20, offset: int = 0) -> dict:
        return {"limit": min(limit, self.max_limit), "offset": offset}

# Usage in endpoint
@app.get("/users")
async def list_users(
    pagination: dict = Depends(Pagination(max_limit=50)),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    users = await db.execute(select(User).limit(pagination["limit"]).offset(pagination["offset"]))
    return users.scalars().all()
```

**DI Resolution Graph:**
```
Endpoint(list_users)
    │
    ├── Depends(Pagination) ──► Pagination().__call__(limit=20, offset=0)
    │
    ├── Depends(get_current_user)
    │       │
    │       ├── Header("authorization") ──► extract from scope
    │       │
    │       └── Depends(get_db)
    │               │
    │               └── async_session() ──► yield session
    │
    └── Depends(get_db) ──► SAME INSTANCE (cached per request!)
```

**Key: `Depends` cache per request** — same dependency = same instance.

---

### 8.4 Pydantic V2 — Validation Pipeline

```python
from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional
from datetime import date

class LoanApplicationCreate(BaseModel):
    amount: float = Field(gt=0, le=200_000)
    term_months: int = Field(ge=3, le=60)
    monthly_income: float = Field(gt=0)
    applicant_age: int = Field(ge=18, le=75)
    document_id: Optional[str] = None
    
    @field_validator('amount')
    @classmethod
    def round_amount(cls, v: float) -> float:
        return round(v, 2)
    
    @model_validator(mode='after')
    def check_dti(self) -> 'LoanApplicationCreate':
        dti = (self.amount / self.term_months) / self.monthly_income
        if dti > 0.6:
            raise ValueError("DTI exceeds 60%")
        return self

# Usage in FastAPI
@app.post("/loans")
async def create_loan(loan: LoanApplicationCreate):
    # loan is ALREADY validated and typed
    # loan.amount → float, loan.term_months → int
    pass
```

**Validation Order:**
1. **Type coercion** (str → int, str → datetime)
2. **Field validators** (`@field_validator`)
3. **Model validators** (`@model_validator`)
4. **Custom types** (`EmailStr`, `HttpUrl`, `UUID4`)

---

### 8.5 BackgroundTasks vs Lifespan vs Celery/Kafka

```python
from fastapi import BackgroundTasks

@app.post("/send-email")
async def send_email(email: str, background_tasks: BackgroundTasks):
    background_tasks.add_task(send_email_task, email)
    return {"status": "queued"}

# Lifespan (app startup/shutdown)
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    # STARTUP
    app.state.db_pool = await create_pool()
    app.state.kafka = await create_kafka_producer()
    yield
    # SHUTDOWN
    await app.state.db_pool.close()
    await app.state.kafka.stop()

app = FastAPI(lifespan=lifespan)
```

| Mechanism | Use Case | Runs In |
|-----------|----------|---------|
| `BackgroundTasks` | Fire-and-forget, same process | After response, same worker |
| `lifespan` | DB pools, Kafka, Redis connections | Startup/Shutdown (once per worker) |
| `ProcessPoolExecutor` | CPU-heavy (ML) | Separate processes |
| Celery/Kafka | Distributed, durable, retry | Separate workers/services |

---

## Part 9: Django vs FastAPI — Architectural Comparison

| Aspect | Django (WSGI) | FastAPI (ASGI) |
|--------|---------------|----------------|
| **Concurrency** | Process-per-request (sync) | Event-loop-per-worker (async) |
| **I/O Model** | Blocking | Non-blocking (async/await) |
| **Request Handling** | 1 worker = 1 request | 1 worker = 1000+ concurrent |
| **Thread Safety** | Per-request thread local | Single-threaded event loop |
| **Middleware** | Sync, wraps request/response | Sync or async, ASGI middleware |
| **DI** | Manual (service locator pattern) | Built-in `Depends()` graph |
| **Validation** | Forms / Serializers (DRF) | Pydantic (types = validation) |
| **OpenAPI** | drf-spectacular (external) | Built-in (`/openapi.json`) |
| **Admin Panel** | Built-in (huge productivity) | None (build your own) |
| **ORM** | Django ORM (sync, async 4.1+) | SQLAlchemy 2.0 async (native) |
| **Learning Curve** | Steep (batteries included) | Gentle (modern Python) |

---

### 9.1 When to Choose What

```
Choose DJANGO when:
├── CRUD-heavy admin panel needed
├── Team knows Django, rapid prototyping
├── Monolith with complex business logic
├── Django packages ecosystem (allauth, wagtail, cms)
└── Traditional sync architecture OK

Choose FASTAPI when:
├── High concurrency (WebSockets, SSE, 10k+ connections)
├── Microservices, event-driven architecture
├── ML/AI inference (async + ProcessPoolExecutor)
├── Team knows async Python, type hints
├── Modern stack: Pydantic, SQLAlchemy 2.0, asyncpg
└── Need auto OpenAPI + client generation
```

---

## Part 10: Production Deployment — Both Stacks

### 10.1 Django Production (Gunicorn + WSGI)

```dockerfile
# Django Dockerfile
FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev gettext && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml ./
RUN pip install --no-cache-dir -e .

COPY src ./src
RUN python manage.py collectstatic --noinput

USER appuser
EXPOSE 8000

# Gunicorn WSGI
CMD ["gunicorn", \
     "--workers", "4", \
     "--bind", "0.0.0.0:8000", \
     "--timeout", "30", \
     "--graceful-timeout", "10", \
     "--max-requests", "5000", \
     "--max-requests-jitter", "1000", \
     "myproject.wsgi:application"]
```

### 10.2 FastAPI Production (Gunicorn + UvicornWorker)

```dockerfile
# FastAPI Dockerfile (identical structure, different worker class)
FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml ./
RUN pip install --no-cache-dir -e .

COPY src ./src
COPY gunicorn.conf.py ./

USER appuser
EXPOSE 8000

CMD ["gunicorn", "-c", "gunicorn.conf.py", "app:app"]
```

```python
# gunicorn.conf.py (FastAPI)
import os, multiprocessing

bind = "0.0.0.0:8000"
workers = int(os.getenv("WEB_CONCURRENCY", multiprocessing.cpu_count() * 2))
worker_class = "uvicorn.workers.UvicornWorker"  # ← KEY DIFFERENCE
worker_connections = 1000
worker_tmp_dir = "/dev/shm"
preload_app = True
max_requests = 5000
max_requests_jitter = 1000
timeout = 30
graceful_timeout = 10
accesslog = "-"
errorlog = "-"
loglevel = "info"
```

---

## Part 11: Debugging & Profiling — Both

### 11.1 Django Debug Toolbar
```python
# settings.py (dev only)
if DEBUG:
    INSTALLED_APPS += ['debug_toolbar']
    MIDDLEWARE = ['debug_toolbar.middleware.DebugToolbarMiddleware'] + MIDDLEWARE
    INTERNAL_IPS = ['127.0.0.1']
```
Shows: SQL queries, templates, cache, signals, request/response

### 11.2 FastAPI/Starlette Debug
```python
# Middleware for timing
@app.middleware("http")
async def add_process_time_header(request, call_next):
    import time
    start = time.perf_counter()
    response = await call_next(request)
    response.headers["X-Process-Time"] = str(time.perf_counter() - start)
    return response
```

### 11.3 Profiling Tools

```bash
# CPU Profiling
python -m cProfile -o profile.stats manage.py runserver  # Django
python -m cProfile -o profile.stats -m uvicorn app:app   # FastAPI

# Analyze
python -c "import pstats; p = pstats.Stats('profile.stats'); p.sort_stats('cumulative').print_stats(20)"

# Memory
pip install memray
memray run manage.py runserver
memray flamegraph profile.bin
```

---

## Summary: Full Stack Mental Model

```
┌────────────────────────────────────────────────────────────────────┐
│                        REQUEST FLOWS                                │
├────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  DJANGO (WSGI)                          FASTAPI (ASGI)            │
│  ─────────────────                      ────────────────          │
│                                                                     │
│  Gunicorn Master                        Gunicorn Master            │
│       │                                      │                     │
│       ▼                                      ▼                     │
│  Worker 1 (sync)                       Worker 1 (UvicornWorker)   │
│  ┌─────────────────┐                   ┌─────────────────┐        │
│  │ Thread 1        │                   │ Event Loop      │        │
│  │ 1. WSGIHandler  │                   │ 1. Starlette    │        │
│  │ 2. Middleware[] │                   │ 2. DI Resolution│        │
│  │ 3. URL Resolve  │                   │ 3. Pydantic     │        │
│  │ 4. View (sync)  │                   │ 4. Endpoint     │        │
│  │ 5. Middleware[] │                   │    (async/sync) │        │
│  │ 6. Response     │                   │ 5. Background   │        │
│  └─────────────────┘                   └─────────────────┘        │
│                                                                     │
│  Scales: More Workers                  Scales: More Workers       │
│          (memory heavy)                (memory efficient)          │
│                                                                     │
└────────────────────────────────────────────────────────────────────┘
```

---

## Recommended Learning Path

1. **Week 1-2:** Python internals (GIL, bytecode, memory, asyncio)
2. **Week 3:** Build raw socket → threading → multiprocessing → asyncio servers
3. **Week 4:** Implement mini-WSGI framework + mini-ASGI framework
4. **Week 5:** Django internals (middleware, ORM, migrations, admin)
5. **Week 6:** FastAPI internals (DI, Pydantic, Starlette, OpenAPI)
6. **Week 7:** Production: Gunicorn config, Docker, healthchecks, observability
8. **Week 8:** Your CrediGuard project — map every component

---

*Generated for CrediGuard project — Senior Python Developer preparation.*