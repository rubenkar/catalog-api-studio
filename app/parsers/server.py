"""
Lightweight HTTP server for the parsers container.
Accepts start/stop commands from the admin panel.

Process keys: "parser" for default (all stages), "parser:stage" for specific stage.
This allows running e.g. podshipnik (items) and podshipnik:details in parallel.

Watchdog: monitors running scrapers, detects stuck processes (no progress for N seconds),
auto-restarts them with resume.
"""
import json
import os
import signal
import subprocess
import sys
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path

PARSERS_DIR = Path('/app')
processes: dict[str, subprocess.Popen] = {}
log_handles: dict[str, object] = {}  # keep file handles to avoid GC
lock = threading.Lock()

# Watchdog tracking: key → {items_file_mtime, last_change_time, start_args, start_cwd}
watchdog_state: dict[str, dict] = {}
STUCK_TIMEOUT = 300  # 5 minutes without items.json change = stuck
MAX_AUTO_RESTARTS = 3  # max consecutive auto-restarts per process key
auto_restart_counts: dict[str, int] = {}


def _proc_key(parser: str, stage: str = '') -> str:
    """Build process key: 'parser' or 'parser:stage'."""
    return f"{parser}:{stage}" if stage and stage != 'all' else parser


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        # Suppress default access logs
        pass

    def _json(self, code: int, data: dict):
        body = json.dumps(data, ensure_ascii=False).encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self) -> dict:
        length = int(self.headers.get('Content-Length', 0))
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        return json.loads(raw)

    def do_GET(self):
        if self.path == '/health':
            self._json(200, {'ok': True})
            return

        if self.path == '/status':
            self._json(200, self._all_status())
            return

        if self.path == '/diagnose':
            self._json(200, self._diagnose())
            return

        self._json(404, {'error': 'Not found'})

    def do_POST(self):
        if self.path == '/start':
            self._handle_start()
            return

        if self.path == '/stop':
            self._handle_stop()
            return

        self._json(404, {'error': 'Not found'})

    def _handle_start(self):
        body = self._read_body()
        parser = body.get('parser', '')
        stage = body.get('stage', '')  # '', 'all', 'items', 'details', etc.
        delay = body.get('delay', '1.0')
        fresh = body.get('fresh', False)

        if not parser or '/' in parser or '.' in parser:
            self._json(400, {'success': False, 'message': 'Invalid parser name'})
            return

        parser_dir = PARSERS_DIR / parser
        scraper_script = parser_dir / 'scraper.py'
        if not scraper_script.exists():
            self._json(404, {'success': False, 'message': 'Scraper script not found'})
            return

        key = _proc_key(parser, stage)

        with lock:
            # Check if already running
            if key in processes:
                proc = processes[key]
                if proc.poll() is None:
                    self._json(409, {
                        'success': False,
                        'message': f'Scraper already running ({key})',
                        'pid': proc.pid,
                    })
                    return

            output_dir = parser_dir / 'output'
            output_dir.mkdir(exist_ok=True)

            # Fresh start: clean old output files so live mode doesn't show stale data
            if fresh:
                for old_file in ['items.json', 'details.json', 'scrape_state.json']:
                    old_path = output_dir / old_file
                    if old_path.exists():
                        old_path.unlink()

            # Remove leftover stop signal (use stage-specific file for parallel runs)
            stop_file = output_dir / (f'.stop_{stage}' if stage and stage != 'all' else '.stop')
            if stop_file.exists():
                stop_file.unlink()
            # Also clean generic .stop if starting the default run
            generic_stop = output_dir / '.stop'
            if generic_stop.exists():
                generic_stop.unlink()

            # Log file: stage-specific for parallel runs
            log_suffix = f'_{stage}' if stage and stage != 'all' else '_full'
            log_file = output_dir / f'scraper{log_suffix}.log'

            args = ['python3', '-u', 'scraper.py', '--output-dir', './output',
                    '--delay', str(delay)]
            if stage and stage != 'all':
                args.extend(['--stage', stage])
            if fresh:
                args.append('--no-resume')

            log_fh = open(log_file, 'w')
            proc = subprocess.Popen(
                args,
                cwd=str(parser_dir),
                stdout=log_fh,
                stderr=subprocess.STDOUT,
                env={**os.environ, 'PYTHONUNBUFFERED': '1'},
            )

            processes[key] = proc
            log_handles[key] = log_fh

            # Track for watchdog
            items_file = output_dir / 'items.json'
            watchdog_state[key] = {
                'items_file': str(items_file),
                'items_mtime': items_file.stat().st_mtime if items_file.exists() else 0,
                'last_change_time': time.time(),
                'start_args': args,
                'start_cwd': str(parser_dir),
                'log_file': str(log_file),
                'parser': parser,
                'stage': stage,
                'delay': delay,
            }
            if fresh:
                auto_restart_counts[key] = 0

            # Write PID file (stage-specific)
            pid_suffix = f'.scraper_{stage}.pid' if stage and stage != 'all' else '.scraper.pid'
            pid_file = output_dir / pid_suffix
            pid_file.write_text(str(proc.pid))

        self._json(200, {
            'success': True,
            'pid': proc.pid,
            'parser': parser,
            'stage': stage or 'all',
            'key': key,
        })

    def _handle_stop(self):
        body = self._read_body()
        parser = body.get('parser', '')
        stage = body.get('stage', '')

        if not parser:
            self._json(400, {'success': False, 'message': 'Missing parser'})
            return

        key = _proc_key(parser, stage)
        killed = False
        pid = None

        with lock:
            # Write stop signal file
            output_dir = PARSERS_DIR / parser / 'output'
            if output_dir.is_dir():
                stop_name = f'.stop_{stage}' if stage and stage != 'all' else '.stop'
                (output_dir / stop_name).write_text(str(os.getpid()))
                # Also write generic .stop for backwards compat
                (output_dir / '.stop').write_text(str(os.getpid()))

            if key in processes:
                proc = processes[key]
                pid = proc.pid
                if proc.poll() is None:
                    # Wait for graceful shutdown via .stop file (up to 15s)
                    try:
                        proc.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        proc.terminate()
                        try:
                            proc.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                    killed = True
                del processes[key]

                # Close log handle
                if key in log_handles:
                    try:
                        log_handles[key].close()
                    except Exception:
                        pass
                    del log_handles[key]

            # Clean up PID file
            pid_suffix = f'.scraper_{stage}.pid' if stage and stage != 'all' else '.scraper.pid'
            pid_file = output_dir / pid_suffix
            if pid_file.exists():
                pid_file.unlink()

        self._json(200, {'success': True, 'pid': pid, 'killed': killed})

    def _all_status(self) -> dict:
        result = {}
        with lock:
            for key, proc in list(processes.items()):
                running = proc.poll() is None
                # Parse key into parser + stage
                if ':' in key:
                    parser, stage = key.split(':', 1)
                else:
                    parser, stage = key, 'all'

                entry = {
                    'parser': parser,
                    'stage': stage,
                    'pid': proc.pid,
                    'running': running,
                    'returncode': proc.returncode,
                }

                # Add watchdog info for running processes
                if running and key in watchdog_state:
                    ws = watchdog_state[key]
                    items_file = Path(ws['items_file'])
                    current_mtime = items_file.stat().st_mtime if items_file.exists() else 0
                    since_change = time.time() - ws['last_change_time']
                    entry['since_last_progress'] = int(since_change)
                    entry['auto_restarts'] = auto_restart_counts.get(key, 0)

                result[key] = entry

                if not running:
                    # Clean up finished processes
                    pid_suffix = f'.scraper_{stage}.pid' if stage != 'all' else '.scraper.pid'
                    pid_file = PARSERS_DIR / parser / 'output' / pid_suffix
                    if pid_file.exists():
                        pid_file.unlink()
                    # Close log handle
                    if key in log_handles:
                        try:
                            log_handles[key].close()
                        except Exception:
                            pass
                        del log_handles[key]
                    # Clean watchdog
                    watchdog_state.pop(key, None)
        return {'success': True, 'data': result}

    def _diagnose(self) -> dict:
        """Diagnose running scrapers: detect stuck, show progress timeline."""
        diag = {}
        now = time.time()

        with lock:
            for key, proc in list(processes.items()):
                running = proc.poll() is None
                if ':' in key:
                    parser, stage = key.split(':', 1)
                else:
                    parser, stage = key, 'all'

                entry = {
                    'parser': parser,
                    'stage': stage,
                    'pid': proc.pid,
                    'running': running,
                    'returncode': proc.returncode,
                    'status': 'ok',
                    'issues': [],
                }

                if not running:
                    entry['status'] = 'exited'
                    diag[key] = entry
                    continue

                ws = watchdog_state.get(key)
                if not ws:
                    entry['issues'].append('No watchdog state — cannot diagnose')
                    diag[key] = entry
                    continue

                # Check items.json progress
                items_file = Path(ws['items_file'])
                if items_file.exists():
                    current_mtime = items_file.stat().st_mtime
                    file_size = items_file.stat().st_size
                    entry['items_file_size'] = file_size
                    entry['items_file_mtime'] = int(current_mtime)

                    if current_mtime != ws['items_mtime']:
                        # File changed — update tracking
                        ws['items_mtime'] = current_mtime
                        ws['last_change_time'] = now
                else:
                    entry['items_file_size'] = 0

                since_change = now - ws['last_change_time']
                entry['seconds_since_progress'] = int(since_change)
                entry['auto_restarts'] = auto_restart_counts.get(key, 0)

                # Check log tail for clues
                log_tail = _read_log_tail(ws.get('log_file', ''), lines=5)
                entry['log_tail'] = log_tail

                # Diagnose issues
                if since_change > STUCK_TIMEOUT:
                    entry['status'] = 'stuck'
                    entry['issues'].append(
                        f'No items.json change for {int(since_change)}s '
                        f'(threshold: {STUCK_TIMEOUT}s)'
                    )
                elif since_change > STUCK_TIMEOUT * 0.6:
                    entry['status'] = 'slow'
                    entry['issues'].append(
                        f'items.json unchanged for {int(since_change)}s — may be stuck soon'
                    )

                # Check if process is consuming CPU (basic: is it responsive?)
                try:
                    os.kill(proc.pid, 0)  # check if process exists
                except OSError:
                    entry['status'] = 'zombie'
                    entry['issues'].append('Process not found but Popen thinks it is running')

                diag[key] = entry

        return {'success': True, 'data': diag}


def _read_log_tail(log_path: str, lines: int = 5) -> list[str]:
    """Read last N lines from a log file."""
    try:
        p = Path(log_path)
        if not p.exists():
            return []
        text = p.read_text(encoding='utf-8', errors='replace')
        all_lines = text.strip().split('\n')
        return all_lines[-lines:]
    except Exception:
        return []


def _watchdog_loop():
    """Background thread: detect stuck scrapers and auto-restart them."""
    print("[watchdog] Started", flush=True)
    while True:
        time.sleep(30)  # check every 30 seconds
        now = time.time()
        to_restart = []

        with lock:
            for key, proc in list(processes.items()):
                if proc.poll() is not None:
                    continue  # already exited
                ws = watchdog_state.get(key)
                if not ws:
                    continue

                # Check if items.json mtime changed
                items_file = Path(ws['items_file'])
                if items_file.exists():
                    current_mtime = items_file.stat().st_mtime
                    if current_mtime != ws['items_mtime']:
                        ws['items_mtime'] = current_mtime
                        ws['last_change_time'] = now
                        auto_restart_counts[key] = 0  # reset on real progress
                        continue

                since_change = now - ws['last_change_time']
                if since_change < STUCK_TIMEOUT:
                    continue

                # Stuck! Check restart budget
                restarts = auto_restart_counts.get(key, 0)
                if restarts >= MAX_AUTO_RESTARTS:
                    print(f"[watchdog] {key}: stuck for {int(since_change)}s but "
                          f"max restarts ({MAX_AUTO_RESTARTS}) reached — giving up",
                          flush=True)
                    continue

                print(f"[watchdog] {key}: stuck for {int(since_change)}s — "
                      f"auto-restarting (attempt {restarts + 1}/{MAX_AUTO_RESTARTS})",
                      flush=True)
                to_restart.append((key, ws.copy()))

        # Do restarts outside lock to avoid deadlock
        for key, ws in to_restart:
            _auto_restart(key, ws)


def _auto_restart(key: str, ws: dict):
    """Kill stuck process and restart with resume."""
    with lock:
        if key in processes:
            proc = processes[key]
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
            del processes[key]

            # Close old log handle
            if key in log_handles:
                try:
                    log_handles[key].close()
                except Exception:
                    pass
                del log_handles[key]

        # Rebuild args WITHOUT --no-resume (we want to resume from state)
        args = ['python3', '-u', 'scraper.py', '--output-dir', './output',
                '--delay', str(ws.get('delay', '1.0'))]
        stage = ws.get('stage', '')
        if stage and stage != 'all':
            args.extend(['--stage', stage])
        # No --no-resume: let it resume from saved state

        log_path = Path(ws['log_file'])
        log_fh = open(log_path, 'a')  # append to existing log
        log_fh.write(f"\n\n--- AUTO-RESTART at {time.strftime('%Y-%m-%d %H:%M:%S')} ---\n\n")
        log_fh.flush()

        proc = subprocess.Popen(
            args,
            cwd=ws['start_cwd'],
            stdout=log_fh,
            stderr=subprocess.STDOUT,
            env={**os.environ, 'PYTHONUNBUFFERED': '1'},
        )

        processes[key] = proc
        log_handles[key] = log_fh
        auto_restart_counts[key] = auto_restart_counts.get(key, 0) + 1

        # Reset watchdog timer
        items_file = Path(ws['items_file'])
        watchdog_state[key] = {
            **ws,
            'items_mtime': items_file.stat().st_mtime if items_file.exists() else 0,
            'last_change_time': time.time(),
        }

        print(f"[watchdog] {key}: restarted as PID {proc.pid} "
              f"(restart #{auto_restart_counts[key]})", flush=True)


def main():
    port = int(os.environ.get('PARSERS_PORT', 8100))

    # Start watchdog thread
    wd = threading.Thread(target=_watchdog_loop, daemon=True)
    wd.start()

    server = HTTPServer(('0.0.0.0', port), Handler)
    print(f"Parsers server listening on port {port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        # Kill all running scrapers
        with lock:
            for key, proc in processes.items():
                if proc.poll() is None:
                    proc.terminate()
        server.server_close()


if __name__ == '__main__':
    main()
