import argparse
import sys
import requests


DEFAULT_ENDPOINTS = ["/health", "/stats", "/anomalies?limit=5", "/metrics/summary?days=7"]


def main() -> int:
    parser = argparse.ArgumentParser(description="Simple API smoke test")
    parser.add_argument("--base-url", default="http://localhost:8000", help="API base URL")
    parser.add_argument("--timeout", type=float, default=5.0, help="Request timeout (seconds)")
    args = parser.parse_args()

    ok = True
    for endpoint in DEFAULT_ENDPOINTS:
        url = f"{args.base_url}{endpoint}"
        try:
            res = requests.get(url, timeout=args.timeout)
            if res.status_code >= 400:
                ok = False
                print(f"[FAIL] {url} -> {res.status_code}")
            else:
                print(f"[OK]   {url} -> {res.status_code}")
        except Exception as exc:
            ok = False
            print(f"[FAIL] {url} -> {exc}")

    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
