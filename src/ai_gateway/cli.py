from __future__ import annotations

import argparse
import json
import sys

from .config import ConfigError, load_config_or_env
from .models import ChatRequest
from .router import Router
from .service import GatewayService, serve


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ai-gateway", description="One OpenAI-compatible endpoint for multiple LLM providers.")
    parser.add_argument("--version", action="version", version="%(prog)s 0.1.2")
    parser.add_argument("command", choices=("serve", "check-config"), nargs="?", default="serve")
    parser.add_argument("--config", help="JSON configuration path; omit to use environment mode")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        config = load_config_or_env(args.config)
    except ConfigError as exc:
        print(f"ai-gateway: {exc}", file=sys.stderr)
        return 2
    if args.command == "check-config":
        print(json.dumps({"valid": True, "providers": [provider.name for provider in config.providers]}))
        return 0
    serve(GatewayService(Router(config), config.server.api_key), config.server.host, config.server.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
