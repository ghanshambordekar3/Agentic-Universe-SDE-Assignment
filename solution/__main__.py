"""
CLI entry point for the durable campaign dispatcher.
Usage:
  python -m solution [--fixtures ...] [--provider-url ...] [--out ...]
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys

from .config import SystemConfig
from .runner import Runner

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_FIXTURES = os.path.join(HERE, "..", "assignment-b-workflow", "fixtures", "campaigns.json")


def main():
    parser = argparse.ArgumentParser(description="Agentic Universe - Durable Campaign Dispatcher")
    parser.add_argument(
        "--fixtures",
        default=DEFAULT_FIXTURES,
        help="Path to campaigns.json fixtures file",
    )
    parser.add_argument(
        "--provider-url",
        default="http://127.0.0.1:8099",
        help="Telephony provider base URL (default: http://127.0.0.1:8099)",
    )
    parser.add_argument(
        "--webhook-port",
        type=int,
        default=8200,
        help="Local port to receive telephony webhooks (default: 8200)",
    )
    parser.add_argument(
        "--db-path",
        default="campaign_dispatcher.db",
        help="Path to SQLite database file (default: campaign_dispatcher.db)",
    )
    parser.add_argument(
        "--out",
        default="final_state.csv",
        help="Output CSV file path (default: final_state.csv)",
    )

    args = parser.parse_args()

    if not os.path.exists(args.fixtures):
        print(f"Error: fixtures file not found at {args.fixtures}", file=sys.stderr)
        sys.exit(1)

    cfg = SystemConfig.load(
        fixtures_path=args.fixtures,
        provider_url=args.provider_url,
        webhook_port=args.webhook_port,
        db_path=args.db_path,
    )

    runner = Runner(cfg)
    try:
        asyncio.run(runner.run(out_path=args.out))
    except KeyboardInterrupt:
        print("\nShutdown requested by user. Exiting.")


if __name__ == "__main__":
    main()
