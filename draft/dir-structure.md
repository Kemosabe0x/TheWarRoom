```
the-war-room/
├── server.py             # Main entry point (the code below)
├── pyproject.toml        # Dependencies (fastmcp, pydantic, nflreadpy, etc.)
├── betting/
│   ├── kalshi.py         # 15-min markets, combo bets, latency pricing
│   └── monitor.py        # Spreads and moneylines logic
├── fantasy/
│   ├── sleeper.py        # Sleeper API integration
│   └── projections.py    # nflreadpy and NHL next-gen stats
└── content/
    └── generator.py      # Social feed and blog post creation

```
