import subprocess
import threading
import sys
import os
import time
import argparse
import signal

RESET  = "\033[0m"
BOLD   = "\033[1m"
COLORS = [
    "\033[36m",   # cyan        → stream processor
    "\033[32m",   # green       → notification consumer
    "\033[33m",   # yellow      → inventory consumer
    "\033[35m",   # magenta     → DLQ handler
    "\033[34m",   # blue        → PG sink
    "\033[95m",   # light magenta → PG DLQ sink
    "\033[91m",   # light red   → producer (last, most visible)
]

COMPONENTS = [
    {
        "name":          "StreamProcessor",
        "cmd":           [sys.executable, "streams/order_processor.py", "worker", "-l", "info"],
        "startup_delay": 0,
    },
    {
        "name":          "NotificationSvc",
        "cmd":           [sys.executable, "consumers/notification_consumer.py"],
        "startup_delay": 3,
    },
    {
        "name":          "InventorySvc",
        "cmd":           [sys.executable, "consumers/inventory_consumer.py"],
        "startup_delay": 3,
    },
    {
        "name":          "DLQ-Handler",
        "cmd":           [sys.executable, "consumers/dlq_handler.py"],
        "startup_delay": 3,
    },
    {
        "name":          "PG-Sink",
        "cmd":           [sys.executable, "consumers/pg_sink_consumer.py"],
        "startup_delay": 3,
    },
    {
        "name":          "PG-DLQ-Sink",
        "cmd":           [sys.executable, "consumers/pg_dlq_sink_consumer.py"],
        "startup_delay": 3,
    },
    {
        "name":          "Producer",
        "cmd":           [sys.executable, "producers/order_producer.py"],
        "startup_delay": 10,
    },
]

processes: list[subprocess.Popen] = []
shutdown_event = threading.Event()

def pad(name: str, width: int = 16) -> str:
    return name[:width].ljust(width)


def stream_output(proc: subprocess.Popen, label: str, color: str):
    prefix = f"{color}{BOLD}[{pad(label)}]{RESET} "
    try:
        for line in proc.stdout:
            if shutdown_event.is_set():
                break
            print(prefix + line.rstrip(), flush=True)
    except Exception:
        pass


def run_setup():
    print(f"\n{BOLD}Step 1: Ensuring Kafka topics exist{RESET}\n")
    result = subprocess.run(
        [sys.executable, "config/setup_topics.py"],
        cwd=os.path.dirname(os.path.abspath(__file__)),
    )
    if result.returncode != 0:
        print(f"\nTopic setup failed. Is Kafka running on your VM?")
        sys.exit(1)
    print(f"\n{BOLD}Step 2: Starting pipeline components{RESET}\n")


def start_component(component: dict, color: str) -> subprocess.Popen:
    """Launch a single component subprocess and wire up its output thread."""
    name  = component["name"]
    cmd   = component["cmd"]
    delay = component["startup_delay"]

    if delay > 0:
        time.sleep(delay)

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,   # merge stderr into stdout
        text=True,
        bufsize=1,                  # line-buffered
        cwd=os.path.dirname(os.path.abspath(__file__)),
    )
    processes.append(proc)

    # Spin up a daemon thread to forward this process's output
    t = threading.Thread(
        target=stream_output,
        args=(proc, name, color),
        daemon=True,
    )
    t.start()

    print(f"{color}{BOLD}[{pad(name)}]{RESET} ▶ Started (pid={proc.pid})")
    return proc


def shutdown(signum=None, frame=None):
    """Gracefully stop all subprocesses on Ctrl+C or SIGTERM."""
    if shutdown_event.is_set():
        return
    shutdown_event.set()

    print(f"\n\n{BOLD}Shutting down pipeline (Ctrl+C received){RESET}\n")

    for proc in reversed(processes):   # stop producer first
        if proc.poll() is None:
            try:
                proc.send_signal(signal.SIGINT)
            except Exception:
                pass

    # Give processes up to 5 seconds to exit cleanly
    deadline = time.monotonic() + 5
    for proc in processes:
        remaining = max(0, deadline - time.monotonic())
        try: 
            proc.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            proc.kill()

    print(f"\n{BOLD}All components stopped.{RESET}\n")
    sys.exit(0)


def main():
    parser = argparse.ArgumentParser(description="Kafka Order Pipeline launcher")
    parser.add_argument(
        "--no-producer",
        action="store_true",
        help="Start everything except the producer (useful for testing consumers first)",
    )
    parser.add_argument(
        "--skip-setup",
        action="store_true",
        help="Skip the topic creation step (if topics already exist)",
    )
    args = parser.parse_args()

    signal.signal(signal.SIGINT,  shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    print(f"""
{BOLD}╔══════════════════════════════════════════════╗
║       Kafka Order Pipeline — Launcher        ║
╚══════════════════════════════════════════════╝{RESET}

  Components that will start:
""")

    components = COMPONENTS
    if args.no_producer:
        components = [c for c in COMPONENTS if c["name"] != "Producer"]
        print(f"  --no-producer flag set: Producer will NOT start\n")

    for i, c in enumerate(components):
        color = COLORS[i % len(COLORS)]
        delay_str = f"(after {c['startup_delay']}s delay)" if c["startup_delay"] else "(immediately)"
        print(f"  {color}{BOLD}[{pad(c['name'])}]{RESET} {' '.join(c['cmd'][1:])} {delay_str}")

    print(f"\n  Press {BOLD}Ctrl+C{RESET} to stop all components.\n")

    # Step 1: topic setup
    if not args.skip_setup:
        run_setup()

    # Step 2: launch components — each in its own thread so delays are parallel
    threads = []
    for i, component in enumerate(components):
        color = COLORS[i % len(COLORS)]
        t = threading.Thread(
            target=start_component,
            args=(component, color),
            daemon=True,
        )
        t.start()
        threads.append(t)

    # Wait for all startup threads to finish launching their processes
    for t in threads:
        t.join()

    print(f"\n{BOLD}All components running — press Ctrl+C to stop{RESET}\n")

    # Keep the main thread alive, watching for any process that dies unexpectedly
    try:
        while not shutdown_event.is_set():
            for proc in processes:
                if proc.poll() is not None and not shutdown_event.is_set():
                    name = next(
                        (c["name"] for c in COMPONENTS if c["cmd"] == proc.args),
                        "unknown"
                    )
                    print(f"\n{BOLD}{name}{RESET} exited unexpectedly (code={proc.returncode})")
            time.sleep(2)
    except KeyboardInterrupt:
        shutdown()


if __name__ == "__main__":
    main()
