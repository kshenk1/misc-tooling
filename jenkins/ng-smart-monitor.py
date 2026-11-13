#!/usr/bin/env python

import os
import csv
import time
import asyncio
import logging
import argparse

from colorama import Fore, Style, init
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit, unquote

import aiohttp

##############################################################################
# Configuration
##############################################################################

TRACKING_INTERVAL = 20
DISCOVERY_INTERVAL = 5
QUEUE_INTERVAL = 3
EXECUTOR_INTERVAL = 5
BUILD_POLL_INTERVAL = 5

API_CONCURRENCY = 25
HTTP_TIMEOUT = 60

OUTPUT_DIR = Path("output")

init(autoreset=True)

##############################################################################
# Logging
##############################################################################

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

log = logging.getLogger(__name__)

##############################################################################
# Models
##############################################################################

@dataclass
class QueueSample:
    timestamp: datetime
    depth: int

@dataclass
class ExecutorSample:
    timestamp: datetime
    total_executors: int
    busy_executors: int
    utilization_pct: float

@dataclass
class BuildRecord:
    controller: str
    job_name: str
    build_number: int
    build_url: str
    result: str
    agent: str
    start_time: datetime
    end_time: datetime
    runtime_seconds: float
    queue_wait_seconds: float | None
    queue_id: int | None = None

@dataclass
class QueueItem:
    queue_id: int
    job_name: str
    enter_time: datetime

@dataclass
class MonitorState:
    active_builds: dict = field(default_factory=dict)
    queue_tracker: dict = field(default_factory=dict)
    completed_builds: list = field(default_factory=list)
    queue_samples: list = field(default_factory=list)
    executor_samples: list = field(default_factory=list)

##############################################################################
# Monitor
##############################################################################

class ControllerMonitor:
    def __init__(self, controller_name, jenkins_url, username, token, duration, drain_time):

        self.controller_name    = controller_name
        self.jenkins_url        = jenkins_url.rstrip("/")
        self.duration           = duration
        self.drain_time         = drain_time

        self.headers        = {"Authorization": aiohttp.encode_basic_auth(username, token)}
        self.state          = MonitorState()
        self.stop_event     = asyncio.Event()
        self.api_limiter    = asyncio.Semaphore(API_CONCURRENCY)

    ##########################################################################
    # Utilities
    ##########################################################################

    async def get_json(self, session, url):
        async with self.api_limiter:
            async with session.get(url) as response:
                response.raise_for_status()
                return await response.json()

    @staticmethod
    def time_display(seconds):
        minutes, _seconds = divmod(seconds, 60)
        return minutes, _seconds, f"{minutes:02d}:{_seconds:02d}"

    @staticmethod
    def utc_now():
        return datetime.now(timezone.utc)

    @staticmethod
    def epoch_ms_to_datetime(ms):
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)

    @staticmethod
    def extract_job_name(build_url):
        parts = [p for p in build_url.split("/") if p]
        jobs = []

        for idx, value in enumerate(parts):
            if value == "job":
                jobs.append(parts[idx + 1])

        return "/".join(jobs[-2:])

    ##########################################################################
    # Queue Monitoring
    ##########################################################################

    async def get_queue_data(self, session):
        tree = "items[id,inQueueSince,task[name,url]]"
        url = f"{self.jenkins_url}/queue/api/json?tree={tree}"
        return await self.get_json(session, url)

    async def sample_queue(self, session):
        while not self.stop_event.is_set():
            try:
                data = await self.get_queue_data(session)
                queue_items = data.get("items", [])

                self.state.queue_samples.append(
                    QueueSample(
                        timestamp=self.utc_now(),
                        depth=len(queue_items)
                    )
                )

                for item in queue_items:
                    queue_id = item.get("id")
                    task = item.get("task", {})
                    name = task.get("name") or task.get("url", "")

                    if (queue_id and queue_id not in self.state.queue_tracker):
                        self.state.queue_tracker[queue_id] = QueueItem(
                            queue_id=queue_id,
                            job_name=name,
                            enter_time=self.utc_now()
                        )

                #log.info("Queue depth: %s", len(queue_items))

            except Exception:
                log.exception(f"{Fore.RED}Queue sampler failed")

            await asyncio.sleep(QUEUE_INTERVAL)

    ##########################################################################
    # Executor Monitoring
    ##########################################################################

    async def sample_executors(self, session):
        tree = "computer[displayName,offline,executors[currentExecutable[url]]]"
        url = f"{self.jenkins_url}/computer/api/json?tree={tree}"

        while not self.stop_event.is_set():
            try:
                data = await self.get_json(session, url)
                total = 0
                busy = 0

                for node in data["computer"]:
                    if node.get("offline"):
                        continue

                    for executor in node.get("executors", []):
                        total += 1

                        if executor.get("currentExecutable"):
                            busy += 1

                utilization = (busy / total) * 100 if total else 0

                self.state.executor_samples.append(
                    ExecutorSample(
                        timestamp=self.utc_now(),
                        total_executors=total,
                        busy_executors=busy,
                        utilization_pct=utilization
                    )
                )

            except Exception:
                log.exception(f"{Fore.RED}Executor sampler failed")

            await asyncio.sleep(EXECUTOR_INTERVAL)

    ##########################################################################
    # Running Build Discovery
    ##########################################################################

    async def discover_builds(self, session):
        tree = "computer[displayName,executors[currentExecutable[url,number,fullDisplayName]]]"
        url = f"{self.jenkins_url}/computer/api/json?tree={tree}"

        while not self.stop_event.is_set():
            try:
                data = await self.get_json(session, url)

                for computer in data["computer"]:
                    agent = computer["displayName"]

                    for executor in computer["executors"]:
                        ce = executor.get("currentExecutable")

                        if not ce:
                            continue

                        build_url       = ce["url"]
                        job_name        = self.extract_job_name(build_url)
                        build_number    = ce.get("number")

                        build_key = (job_name, build_number)

                        if build_key in self.state.active_builds:
                            continue

                        task = asyncio.create_task(
                            self.monitor_build(session, build_url, job_name, build_number, agent)
                        )

                        self.state.active_builds[build_key] = task

                        log.info(f"Tracking {Fore.CYAN}{unquote(unquote(job_name))}{Style.RESET_ALL} #{build_number}")

                await self.harvest_finished()

            except Exception:
                log.exception(f"{Fore.RED}Discovery failure")

            await asyncio.sleep(DISCOVERY_INTERVAL)

    ##########################################################################
    # Build Monitoring
    ##########################################################################

    async def build_tracking(self):
        try:
            while True:
                active      = len(self.state.active_builds)
                completed   = len(self.state.completed_builds)
                queue_depth = self.state.queue_samples[-1].depth if self.state.queue_samples else 0
                elapsed     = int(time.time() - self.start_time)

                if not self.stop_event.is_set():
                    remaining   = max(0, self.duration - elapsed)
                    phase       = f"{Fore.BLUE}MONITORING{Style.RESET_ALL}"
                else:
                    remaining   = max(0, self.drain_time - elapsed)
                    phase       = f"{Fore.YELLOW}DRAINING{Style.RESET_ALL}"

                mins, _, display_time = self.time_display(remaining)

                if mins >= 10:
                    time_str = f"{Fore.GREEN}{display_time}"
                elif mins >= 3:
                    time_str = f"{Fore.YELLOW}{display_time}"
                else:
                    time_str = f"{Fore.RED}{display_time}"

                log.info(
                    f" --> [{phase}] "
                    f"Active: {Style.BRIGHT}{active}{Style.RESET_ALL}, "
                    f"Completed: {Style.BRIGHT}{completed}{Style.RESET_ALL}, "
                    f"Queue Depth: {Style.BRIGHT}{queue_depth}{Style.RESET_ALL}, "
                    f"Remaining: {time_str}{Style.RESET_ALL}"
                )

                await asyncio.sleep(TRACKING_INTERVAL)

        except asyncio.CancelledError:
            log.info(f"{Fore.RED}Stats monitor stopped")
            raise

    async def fetch_build_stats(self, session, build_url):
        tree = "building,result,duration,timestamp,number"
        url = f"{build_url.rstrip('/')}/api/json?tree={tree}"

        return await self.get_json(session, url)

    async def monitor_build(self, session, build_url, job_name, build_number, agent):
        initial = await self.fetch_build_stats(session, build_url)
        start_time = self.epoch_ms_to_datetime(initial["timestamp"])
        queue_wait = None

        while True:
            current = await self.fetch_build_stats(session, build_url)

            if not current["building"]:
                break

            await asyncio.sleep(BUILD_POLL_INTERVAL)

        end_time = self.utc_now()
        runtime_seconds = (current["duration"] / 1000)

        return BuildRecord(
            controller=self.controller_name,
            job_name=job_name,
            build_number=build_number,
            build_url=build_url,
            result=current["result"],
            agent=agent,
            start_time=start_time,
            end_time=end_time,
            runtime_seconds=runtime_seconds,
            queue_wait_seconds=queue_wait,
        )

    ##########################################################################
    # Harvest Completed Tasks
    ##########################################################################

    async def harvest_finished(self):
        completed = []

        for key, task in list(self.state.active_builds.items()):
            if not task.done():
                continue

            completed.append(key)

            if task.cancelled():
                continue

            try:
                record = task.result()
                self.state.completed_builds.append(record)

                if record.result == 'SUCCESS':
                    result = f"{Fore.GREEN}SUCCESS{Style.RESET_ALL}"
                elif record.result == 'FAILURE':
                    result = f"{Fore.RED}FAILURE{Style.RESET_ALL}"
                else:
                    result = f"{Fore.YELLOW}{record.result}{Style.RESET_ALL}"

                log.info(f"Completed: {Fore.CYAN}{unquote(unquote(record.job_name))}{Style.RESET_ALL} #{record.build_number} ({result})")

            except asyncio.CancelledError:
                pass

            except Exception:
                log.exception(f"{Fore.RED}Build monitor failed")

        for key in completed:
            self.state.active_builds.pop(key, None)

    ##########################################################################
    # CSV Output
    ##########################################################################

    def write_csv_files(self):
        OUTPUT_DIR.mkdir(exist_ok=True)

        build_file = OUTPUT_DIR / "builds.csv"
        queue_file = OUTPUT_DIR / "queue.csv"
        executor_file = OUTPUT_DIR / "executors.csv"

        with open(build_file, "w", newline="") as fh:
            writer = csv.DictWriter(fh,
                fieldnames=list(
                    asdict(
                        self.state.completed_builds[0]
                    ).keys()
                ) if self.state.completed_builds else []
            )

            if self.state.completed_builds:
                writer.writeheader()

                for row in self.state.completed_builds:
                    writer.writerow(asdict(row))

        with open(queue_file, "w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=["timestamp","depth"])

            writer.writeheader()

            for row in self.state.queue_samples:
                writer.writerow(asdict(row))

        with open(executor_file, "w", newline="") as fh:

            writer = csv.DictWriter(fh,
                fieldnames=[
                    "timestamp",
                    "total_executors",
                    "busy_executors",
                    "utilization_pct"
                ]
            )

            writer.writeheader()

            for row in self.state.executor_samples:
                writer.writerow(asdict(row))

    ##########################################################################
    # Summary
    ##########################################################################

    def print_summary(self):
        builds      = self.state.completed_builds
        total       = len(builds)
        success     = sum(1 for b in builds if b.result == "SUCCESS")
        failures    = total - success

        avg_runtime = (
            sum(b.runtime_seconds for b in builds) / total
            if total else 0
        )
        _, _, display_time = self.time_display(avg_runtime)

        avg_queue_depth = (
            sum(q.depth for q in self.state.queue_samples) / len(self.state.queue_samples)
            if self.state.queue_samples else 0
        )

        peak_utilization = max(
            (e.utilization_pct for e in self.state.executor_samples), default=0
        )

        print()
        print(f"{Style.BRIGHT}============================")
        print(f"{Style.BRIGHT}Controller Summary")
        print(f"{Style.BRIGHT}============================")
        print(f"{Style.BRIGHT}Builds Observed{Style.RESET_ALL} : {total}")
        print(f"{Style.BRIGHT}Successes{Style.RESET_ALL}       : {success}")
        print(f"{Style.BRIGHT}Failures{Style.RESET_ALL}        : {failures}")
        print(f"{Style.BRIGHT}Avg Runtime(s){Style.RESET_ALL}  : {display_time}")
        print(f"{Style.BRIGHT}Avg Queue Depth{Style.RESET_ALL} : {avg_queue_depth:.2f}")
        print(f"{Style.BRIGHT}Peak Utilization{Style.RESET_ALL}: {peak_utilization:.1f}%")

    ##########################################################################
    # Main Runner
    ##########################################################################

    async def run(self):
        self.start_time = time.time()
        connector = aiohttp.TCPConnector(limit=API_CONCURRENCY, ssl=False)
        timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT)

        async with aiohttp.ClientSession(headers=self.headers, connector=connector, timeout=timeout) as session:
            queue_task      = asyncio.create_task(self.sample_queue(session))
            executor_task   = asyncio.create_task(self.sample_executors(session))
            discovery_task  = asyncio.create_task(self.discover_builds(session))
            tracking_task   = asyncio.create_task(self.build_tracking())
            try:
                await asyncio.sleep(self.duration)

                log.info(f"{Fore.YELLOW}Stopping discovery...")
                self.stop_event.set()

                await queue_task
                await executor_task
                await discovery_task

                if self.state.active_builds:
                    log.info(f"Draining {Fore.YELLOW}{len(self.state.active_builds)}{Style.RESET_ALL} active builds")
                    self.start_time = time.time()

                    try:
                        await asyncio.wait_for(
                            asyncio.gather(*self.state.active_builds.values()), timeout=self.drain_time
                        )
                    except asyncio.TimeoutError:
                        log.warning(f"{Fore.YELLOW}Drain timeout exceeded")
                        for task in self.state.active_builds.values():
                            task.cancel()
                        await asyncio.gather(*self.state.active_builds.values(), return_exceptions=True)

            except asyncio.CancelledError:
                log.warning(f"{Fore.YELLOW}Ctrl-C detected — stopping samplers early and saving collected data...")
                self.stop_event.set()
                background_tasks = [
                    queue_task,
                    executor_task,
                    discovery_task,
                    *self.state.active_builds.values(),
                ]
                for task in background_tasks:
                    task.cancel()
                await asyncio.gather(*background_tasks, return_exceptions=True)

            finally:
                await self.harvest_finished()
                tracking_task.cancel()
                try:
                    await tracking_task
                except asyncio.CancelledError:
                    pass

        self.write_csv_files()
        self.print_summary()

##############################################################################
# CLI
##############################################################################

def parse_args():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter, 
        description="Target a single controller and monitor the running jobs and build queue for stats for a specified length of time."
    )

    parser.add_argument("-j", "--jenkins-url", dest='jenkins_url', required=True,
        help="The Jenkins or CI instance to target. Example: ci.example.com or ci.example.com:8443 (include th port if other than standard)")
    parser.add_argument("-d", "--duration", dest='duration', default=1800, type=int,
        help="The length of time in seconds to run this monitor for.")
    parser.add_argument("--drain-time", dest='drain_time', default=300, type=int,
        help="The length of time in seconds to wait for completion of monitored builds before giving up. This begins when --duration ends.")

    return parser.parse_args()

def normalize_base_url(target: str) -> str:
    target = target.strip()
    if "://" not in target and not target.startswith("//"):
        target = f"//{target}"
    parsed  = urlsplit(target)
    scheme  = parsed.scheme or "https"
    path    = parsed.path.rstrip("/")
    return f"{scheme}://{parsed.netloc}{path}"

async def inspect_cloudbees_target(target: str, username: str, api_token: str) -> dict:
    log.info("Parsing, munging, smelling and inspecting the url...")
    base_url    = normalize_base_url(target)
    headers     = {"Authorization": aiohttp.encode_basic_auth(username, api_token)}
    connector   = aiohttp.TCPConnector(ssl=False)
    timeout     = aiohttp.ClientTimeout(total=10)

    async with aiohttp.ClientSession(headers=headers, connector=connector, timeout=timeout) as session:
        async with session.get(f"{base_url}/api/json", params={"tree": "url,jobs[_class]"}) as root_resp:
            root_resp.raise_for_status()
            data = await root_resp.json()
            jenkins_header = root_resp.headers.get("X-Jenkins", "")

        canonical_url = (data.get("url") or base_url).rstrip("/")
        job_classes = [j.get("_class", "") for j in data.get("jobs", [])]

        is_oc = (
            "Operations Center" in jenkins_header
            or any("opscenter.server.model" in cls for cls in job_classes)
        )

        if not is_oc:
            async with session.get(f"{canonical_url}/casc-bundle/list") as casc_resp:
                if casc_resp.status == 200:
                    is_oc = True

    if is_oc:
        return {
            "type": "operations_center",
            "url": canonical_url,
            "controller_name": None,
        }

    parsed = urlsplit(canonical_url)
    path_segments = [seg for seg in parsed.path.split("/") if seg]
    controller_name = (
        path_segments[-1]
        if path_segments
        else (parsed.hostname.split(".")[0] if parsed.hostname else None)
    )

    return {
        "type": "controller",
        "url": canonical_url,
        "controller_name": controller_name,
    }

async def main():
    args = parse_args()

    username    = os.getenv("JENKINS_API_USERNAME")
    token       = os.getenv("JENKINS_API_TOKEN")

    if not username:
        raise RuntimeError("JENKINS_API_USERNAME missing")

    if not token:
        raise RuntimeError("JENKINS_API_TOKEN missing")

    url_info    = await inspect_cloudbees_target(args.jenkins_url, username, token)

    if url_info['type'] == 'operations_center':
        log.warning(f"URL: {url_info['url']}, Type: {url_info['type']}")
        raise RuntimeError('Currently we can only run this against Controllers')

    log.info(f"Controller: {url_info['controller_name']}, URL: {url_info['url']}, Type: {url_info['type']}")

    monitor = ControllerMonitor(
        controller_name=url_info['controller_name'],
        jenkins_url=url_info['url'],
        username=username,
        token=token,
        duration=args.duration,
        drain_time=args.drain_time,
    )

    await monitor.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.warning("Aborted by user.")
        raise SystemExit(130)
    