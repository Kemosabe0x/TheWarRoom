"""Export advertised schemas through an actual in-process MCP client."""
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from fastmcp import Client
from server import create_server


async def export():
    async with Client(create_server()) as client:
        return {"tools": [t.model_dump(mode="json", exclude_none=True) for t in await client.list_tools()],
                "resources": [r.model_dump(mode="json", exclude_none=True) for r in await client.list_resources()],
                "resource_templates": [r.model_dump(mode="json", exclude_none=True) for r in await client.list_resource_templates()]}


if __name__ == "__main__":
    print(json.dumps(asyncio.run(export()), indent=2))
