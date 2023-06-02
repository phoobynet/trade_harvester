# Trade Harvester

**This is experimental and may suck.**

Capture all trades from the SIP in a single SQLite database.

```bash
go build -ldflags="-s -w"

# Run this before the market opens (pre-market is 0400 in NY)
trade_harvester
```

Generates a database file with a timestamp.

Leave it alone until the market has closed.

Do data science stuff with that data.

