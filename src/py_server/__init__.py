"""MCP-прокси сервер для взаимодействия с 1С."""

from .config import Config, get_config
from .mcp_server import MCPProxy
from .http_server import run_http_server
from .stdio_server import run_stdio_server
from .onec_client import OneCClient, create_onec_client
from .transport import Transport, HttpTransport, FileTransport, FtpFileTransport, ReverseHttpTransport

__version__ = "1.1.0"

__all__ = [
	"Config",
	"get_config",
	"MCPProxy",
	"run_http_server",
	"run_stdio_server",
	"OneCClient",
	"create_onec_client",
	"Transport",
	"HttpTransport",
	"FileTransport",
	"FtpFileTransport",
	"ReverseHttpTransport",
]