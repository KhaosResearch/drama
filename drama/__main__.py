import argparse

from dramatiq.cli import main as dramatiq_cli
from dramatiq.cli import make_argument_parser

from drama import __author__, __version__

HEADER = "\n".join(
    [
        r"    ___  ___  ___   __  ______",
        r"   / _ \/ _ \/ _ | /  |/  / _ |",
        r"  / // / , _/ __ |/ /|_/ / __ |",
        r" /____/_/|_/_/ |_/_/  /_/_/ |_|",
        "",
        f" ver. {__version__}     author {__author__}",
        "",
    ]
)


def get_parser():
    parser = argparse.ArgumentParser(prog="drama")

    subparsers = parser.add_subparsers(dest="command", help="drama sub-commands")
    subparsers.required = True

    subparsers.add_parser("worker", help="Spawn multiple concurrent workers")
    subparsers.add_parser("server", help="Deploy server")

    return parser


def cli():
    print(HEADER)
    args, _ = get_parser().parse_known_args()

    if args.command == "worker":
        dramatiq_ns, _ = make_argument_parser().parse_known_args()
        setattr(dramatiq_ns, "broker", "drama.worker.actor")

        dramatiq_cli(dramatiq_ns)
    elif args.command == "server":
        from drama.api.app import run_server

        run_server()


if __name__ == "__main__":
    cli()
