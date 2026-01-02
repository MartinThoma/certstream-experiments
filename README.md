# CertStream to SQLite

A Python application that connects to a CertStream websocket server and stores certificate transparency logs in an SQLite database.

## Features

- **WebSocket Connection**: Continuously connects to a CertStream server via environment variable
- **Pluggable Storage**: Abstract database interface with SQLite implementation (easily swap for PostgreSQL, MongoDB, etc.)
- **Automatic Schema**: Creates the database and table automatically
- **Domain Indexing**: Each domain gets its own entry with full certificate data
- **Query Tools**: Includes utilities to search and analyze the stored data

## Installation

```bash
pip install -r requirements.txt
```

## Environment Variables

**Required:**
- `CERTSTREAM_WEBSOCKET_URL` - The websocket URL of the CertStream server (e.g., `ws://188.245.146.217:8080/`)

Example:
```bash
export CERTSTREAM_WEBSOCKET_URL="ws://188.245.146.217:8080/"
python certstream_db.py
```

## Usage

### Start Collecting Data

```bash
# Set the environment variable
export CERTSTREAM_WEBSOCKET_URL="ws://188.245.146.217:8080/"

# Run the collector
python certstream_db.py
```

This will:
1. Create `certstream.db` (if it doesn't exist)
2. Connect to the CertStream server using the websocket URL from the environment variable
3. Start receiving and storing certificate updates
4. Continue until interrupted (Ctrl+C)

### Query the Database

```bash
# Show database statistics
python query_db.py --stats

# List 20 most recently updated certificates
python query_db.py --recent 20

# Search for a specific domain
python query_db.py --search example.com

# Use a different database path
python query_db.py --db /path/to/database.db --stats
```

## Database Schema

The SQLite database has a single table `certificates`:

```sql
CREATE TABLE certificates (
    domain TEXT PRIMARY KEY,           -- Domain name (indexed)
    data TEXT NOT NULL,                -- JSON-encoded certificate data
    created_at TIMESTAMP,              -- When first seen
    updated_at TIMESTAMP               -- Last updated time
)
```

### Data Structure Example

The `data` column contains a JSON object:

```json
{
    "domains": ["example.com", "*.example.com"],
    "leaf_cert": {
        "subject": {...},
        "extensions": {...},
        "fingerprint": "..."
    },
    "chain": [...],
    "source": {
        "name": "..."
    },
    "timestamp": "2025-01-02T12:34:56Z"
}
```

## Pluggable Storage Backends

The application uses an abstract `CertificateStore` interface for flexible database support. Included implementations:

- **SQLiteCertificateStore** - SQLite (default)
- **InMemoryCertificateStore** - In-memory storage for testing
- **PostgreSQLCertificateStore** - PostgreSQL example

### Using a Different Backend

```python
from stores_example import InMemoryCertificateStore
from certstream_db import CertStreamCollector
import os

store = InMemoryCertificateStore()
collector = CertStreamCollector(store, websocket_url=os.environ["CERTSTREAM_WEBSOCKET_URL"])
await collector.connect_and_store()
```

See `stores_example.py` for more implementation examples.

## Logging

The application logs to stdout with the following format:
```
2025-01-02 12:34:56,123 - INFO - Database initialized at certstream.db
2025-01-02 12:34:56,234 - INFO - Connected to CertStream at ws://...
2025-01-02 12:35:00,100 - INFO - Processed 100 certificate updates
```

## Performance Notes

- Each certificate update may contain multiple domains, so all are stored
- The database uses `domain` as a primary key, so duplicate domains are automatically updated
- For large deployments, consider adding database indexes or periodic cleanup

## Troubleshooting

- **Connection refused**: Ensure the CertStream server is online and reachable
- **Database locked**: Only one instance of the application should write to the database at a time
- **Memory issues**: Large certificate chains are stored as JSON; consider periodic cleanup or archiving
