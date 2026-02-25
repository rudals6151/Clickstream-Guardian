import argparse
import subprocess
import sys
import time


DEFAULT_SERVICES = ["kafka-2", "schema-registry", "postgres", "spark-streaming"]


def run_cmd(command: list[str], check: bool = True) -> None:
    print(f"[CMD] {' '.join(command)}")
    proc = subprocess.run(command, text=True)
    if check and proc.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(command)}")


def stop_service(service: str, compose_file: str) -> None:
    run_cmd(["docker", "compose", "-f", compose_file, "stop", service])


def start_service(service: str, compose_file: str) -> None:
    run_cmd(["docker", "compose", "-f", compose_file, "start", service])


def restart_service(service: str, compose_file: str) -> None:
    run_cmd(["docker", "compose", "-f", compose_file, "restart", service])


def main() -> int:
    parser = argparse.ArgumentParser(description="Failure simulation utility for Clickstream Guardian")
    parser.add_argument(
        "--compose-file",
        default="docker/docker-compose.yml",
        help="Path to docker-compose file",
    )
    parser.add_argument(
        "--mode",
        choices=["stop-start", "restart"],
        default="stop-start",
        help="Failure simulation mode",
    )
    parser.add_argument(
        "--services",
        nargs="+",
        default=DEFAULT_SERVICES,
        help="Target services",
    )
    parser.add_argument(
        "--down-seconds",
        type=int,
        default=45,
        help="How long to keep service down in stop-start mode",
    )
    args = parser.parse_args()

    try:
        for service in args.services:
            print(f"\n=== Simulating failure for: {service} ===")
            if args.mode == "restart":
                restart_service(service, args.compose_file)
            else:
                stop_service(service, args.compose_file)
                print(f"[INFO] sleeping {args.down_seconds}s while {service} is down")
                time.sleep(args.down_seconds)
                start_service(service, args.compose_file)
            print(f"[OK] completed: {service}")
        return 0
    except Exception as exc:
        print(f"[ERROR] {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
