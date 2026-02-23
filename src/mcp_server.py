"""FastMCP server adapter for patient condition tools.

API notes (mcp>=1.0.0 / FastMCP):
- Tools are registered via the @app.tool() decorator.
- app._tool_manager._tools is a dict[str, Tool] of registered tools.
- app.call_tool(name, args_dict) returns list[TextContent] with JSON payload.
- app.list_tools() returns list[Tool] (async).
- run_stdio_async() is the stdio transport entry-point.
"""
import asyncio
import logging
from typing import Optional

from mcp.server.fastmcp import FastMCP

from src.storage import ConditionStore
from src.mcp_tools import ConditionTools

logger = logging.getLogger(__name__)


def create_mcp_server(
    store: Optional[ConditionStore] = None,
) -> tuple[FastMCP, ConditionStore]:
    """Factory: build and wire a FastMCP app with condition tools.

    Args:
        store: Optional pre-existing ConditionStore; a new one is created if None.

    Returns:
        Tuple of (FastMCP app, ConditionStore).  The store is exposed so callers
        can pre-populate data or run assertions without extra indirection.
    """
    if store is None:
        store = ConditionStore()

    tools = ConditionTools(store)

    app = FastMCP("conditions-server")

    # ------------------------------------------------------------------ #
    # Register query_conditions tool                                       #
    # ------------------------------------------------------------------ #
    @app.tool()
    def query_conditions(
        patient_id: str,
        status: Optional[str] = None,
        code_system: Optional[str] = None,
        code: Optional[str] = None,
        include_lineage: bool = False,
    ) -> dict:
        """RAG tool: query the live representation of patient conditions.

        Returns the cleaned, currently active patient state respecting any
        user corrections.  Masked conditions are excluded from results.
        """
        return tools.query_conditions(
            patient_id=patient_id,
            status=status,
            code_system=code_system,
            code=code,
            include_lineage=include_lineage,
        )

    # ------------------------------------------------------------------ #
    # Register issue_correction tool                                       #
    # ------------------------------------------------------------------ #
    @app.tool()
    def issue_correction(
        patient_id: str,
        condition_ids: list[str],
        reason: Optional[str] = None,
    ) -> dict:
        """Mutation tool: append a correction event to exclude conditions.

        Original clinical records are never deleted; corrections are stored as
        immutable events that filter the live representation.
        """
        return tools.issue_correction(
            patient_id=patient_id,
            condition_ids=condition_ids,
            reason=reason,
        )

    logger.info(
        "MCP server created",
        extra={"tools": list(app._tool_manager._tools.keys())},
    )

    return app, store


def run_server() -> None:
    """Entry-point for `uv run mcp-conditions` CLI script."""
    app, _store = create_mcp_server()
    asyncio.run(app.run_stdio_async())
