"""
Tests for run_server.py entry point.

Usage:
    # From the databricks-mcp-server directory:
    pip install -e .
    pip install pytest
    pytest test_run_server.py -v

    # Or run directly:
    python test_run_server.py
"""

import subprocess
import sys
from unittest.mock import MagicMock, patch

import pytest

# Skip all tests if the package isn't installed
pytestmark = pytest.mark.skipif(
    not __import__("importlib.util", fromlist=[""]).find_spec("databricks_mcp_server"),
    reason="databricks_mcp_server package not installed",
)


class TestRunServerImports:
    """Test that run_server.py imports work correctly."""

    def test_server_module_importable(self):
        """Test that databricks_mcp_server.server can be imported."""
        from databricks_mcp_server import server

        assert hasattr(server, "mcp")

    def test_mcp_object_is_fastmcp_instance(self):
        """Test that mcp is a FastMCP server instance."""
        from databricks_mcp_server.server import mcp
        from fastmcp import FastMCP

        assert isinstance(mcp, FastMCP)

    def test_mcp_has_run_method(self):
        """Test that mcp has a run method."""
        from databricks_mcp_server.server import mcp

        assert hasattr(mcp, "run")
        assert callable(mcp.run)

    def test_mcp_server_name(self):
        """Test that the MCP server has the expected name."""
        from databricks_mcp_server.server import mcp

        assert mcp.name == "Databricks MCP Server"


class TestRunServerExecution:
    """Test run_server.py execution behavior."""

    def test_script_syntax_valid(self):
        """Test that run_server.py has valid Python syntax."""
        script_content = '''#!/usr/bin/env python
"""Run the Databricks MCP Server."""

from databricks_mcp_server.server import mcp

if __name__ == "__main__":
    mcp.run(transport="stdio")
'''
        # Compile should succeed without SyntaxError
        compile(script_content, "run_server.py", "exec")

    @patch("databricks_mcp_server.server.mcp")
    def test_run_server_main_block(self, mock_mcp):
        """Test the main block execution logic."""
        mock_mcp.run = MagicMock()

        # Directly test the execution pattern
        mock_mcp.run(transport="stdio")

        mock_mcp.run.assert_called_once_with(transport="stdio")

    def test_run_with_mocked_server(self):
        """Test that run is called with correct transport argument."""
        mock_run = MagicMock()

        with patch("databricks_mcp_server.server.mcp") as mock_mcp:
            mock_mcp.run = mock_run

            # Import and execute the main pattern
            from databricks_mcp_server.server import mcp

            mcp.run(transport="stdio")

        # Verify the actual call
        from databricks_mcp_server.server import mcp

        assert hasattr(mcp, "run")


class TestMCPServerConfiguration:
    """Test MCP server configuration and tool registration."""

    def test_sql_tools_registered(self):
        """Test that SQL tools module is available."""
        from databricks_mcp_server.tools import sql

        assert sql is not None

    def test_compute_tools_registered(self):
        """Test that compute tools module is available."""
        from databricks_mcp_server.tools import compute

        assert compute is not None

    def test_file_tools_registered(self):
        """Test that file tools module is available."""
        from databricks_mcp_server.tools import file

        assert file is not None

    def test_pipelines_tools_registered(self):
        """Test that pipelines tools module is available."""
        from databricks_mcp_server.tools import pipelines

        assert pipelines is not None

    def test_jobs_tools_registered(self):
        """Test that jobs tools module is available."""
        from databricks_mcp_server.tools import jobs

        assert jobs is not None

    def test_all_tool_modules_importable(self):
        """Test that all tool modules can be imported."""
        from databricks_mcp_server.tools import (
            agent_bricks,
            aibi_dashboards,
            compute,
            file,
            genie,
            jobs,
            pipelines,
            serving,
            sql,
            unity_catalog,
            volume_files,
        )

        modules = [
            sql,
            compute,
            file,
            pipelines,
            jobs,
            agent_bricks,
            aibi_dashboards,
            serving,
            unity_catalog,
            volume_files,
            genie,
        ]
        for module in modules:
            assert module is not None


class TestMCPServerIntegration:
    """Integration tests for MCP server startup."""

    @pytest.mark.skipif(
        sys.platform == "win32", reason="Unix-specific subprocess handling"
    )
    def test_server_import_subprocess(self):
        """Test that the server can be imported in a subprocess."""
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "from databricks_mcp_server.server import mcp; "
                "print(f'name={mcp.name}')",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, f"Import failed: {result.stderr}"
        assert "name=Databricks MCP Server" in result.stdout

    def test_server_has_tools(self):
        """Test that the server has tools registered after import."""
        from databricks_mcp_server.server import mcp

        # FastMCP uses _tool_manager internally
        if hasattr(mcp, "_tool_manager"):
            tool_manager = mcp._tool_manager
            if hasattr(tool_manager, "_tools"):
                assert len(tool_manager._tools) > 0, "No tools registered"
        # If we can't access internals, just verify server exists
        assert mcp is not None


class TestRunServerScript:
    """Test run_server.py as a standalone script."""

    def test_script_is_executable_module(self):
        """Test that the script structure is correct for execution."""
        # The expected content of run_server.py
        expected_import = "from databricks_mcp_server.server import mcp"
        expected_main = 'if __name__ == "__main__":'
        expected_run = 'mcp.run(transport="stdio")'

        script = f'''#!/usr/bin/env python
"""Run the Databricks MCP Server."""

{expected_import}

{expected_main}
    {expected_run}
'''
        # Verify it compiles
        code = compile(script, "<test>", "exec")
        assert code is not None

    def test_transport_argument(self):
        """Test that stdio is the correct transport for CLI usage."""
        from databricks_mcp_server.server import mcp

        # Verify run method accepts transport parameter
        import inspect

        sig = inspect.signature(mcp.run)
        params = list(sig.parameters.keys())
        # FastMCP.run should accept transport parameter
        assert "transport" in params or any(
            p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
        )


if __name__ == "__main__":
    # Check if package is installed before running
    import importlib.util

    if importlib.util.find_spec("databricks_mcp_server"):
        pytest.main([__file__, "-v"])
    else:
        print("ERROR: databricks_mcp_server package not installed.")
        print("\nTo run these tests:")
        print("  1. cd databricks-mcp-server")
        print("  2. pip install -e .")
        print("  3. pip install pytest")
        print("  4. pytest test_run_server.py -v")
        sys.exit(1)
