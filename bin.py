import multiprocessing
import time

from concurrent.futures import ProcessPoolExecutor

import pandas as pd

from copy import deepcopy

def read_pipeline_names(file_path):
    """
    Read the pipeline names from the provided text file and return them as a list.
    The file is expected to contain comma-separated single-quoted strings.
    
    Args:
        file_path (str): Path to the text file containing the pipeline names
        
    Returns:
        list: List of pipeline names
    """
    with open(file_path, 'r') as file:
        file_content = file.read()
    
    # Handle comma-separated values with single quotes
    # Split by commas and clean up each name
    pipeline_names = set()
    for name in file_content.split(','):
        cleaned_name = name.strip().strip("'").strip('"')
        # Skip empty names
        if cleaned_name:
            pipeline_names.add(cleaned_name)
    
    return list(pipeline_names)

def load_csv_file(file_path):
    """
    Load a CSV file into a pandas DataFrame.
    
    Args:
        file_path (str): Path to the CSV file
        
    Returns:
        pd.DataFrame: DataFrame containing the CSV data
    """
    df = pd.read_csv(file_path)
    return df

class Server:
    def __init__(self, id, max_cpus=3, max_memory=16, tag="ec2", dedicated=False, reuse_window=None):
        self.id = id
        self.max_cpus = max_cpus
        self.max_memory = max_memory
        self.tag = tag
        self.dedicated = dedicated
        self.reuse_window = reuse_window
        self._events = []
        self.job_schedule = []
        self.runs = []
        self.creation_time = None


    # ---------- internal helpers ----------

    def _sorted_events(self):
        """Return events sorted by time (stable)."""
        return sorted(self._events, key=lambda e: e[0])

    def _build_usage_profile(self):
        """
        Build piecewise-constant usage profile.
        Returns a list of segments: [(t0, t1, used_cpus, used_mem), ...]
        where [t0, t1) is the time interval with constant usage.
        """
        events = self._sorted_events()
        if not events:
            return []

        # Coalesce events with the same time
        coalesced = []
        i = 0
        n = len(events)
        while i < n:
            t = events[i][0]
            dc, dm = 0, 0
            while i < n and events[i][0] == t:
                dc += events[i][1]
                dm += events[i][2]
                i += 1
            coalesced.append((t, dc, dm))

        # Sweep to build segments
        segments = []
        used_cpus = 0
        used_mem = 0
        for j in range(len(coalesced)):
            t = coalesced[j][0]
            # apply the deltas at t
            used_cpus += coalesced[j][1]
            used_mem += coalesced[j][2]

            # end time is next event time if exists; otherwise open-ended segment
            t_next = coalesced[j + 1][0] if j + 1 < len(coalesced) else None
            if t_next is not None:
                segments.append((t, t_next, used_cpus, used_mem))
            else:
                # final semi-infinite segment: represent as (t, None, ...)
                segments.append((t, None, used_cpus, used_mem))

        return segments

    def _peak_usage(self):
        """Return (max_used_cpus, max_used_mem) over the whole timeline."""
        peak_c, peak_m = 0, 0
        for (t0, t1, uc, um) in self._build_usage_profile():
            peak_c = max(peak_c, uc)
            peak_m = max(peak_m, um)
        return peak_c, peak_m

    # ---------- public scheduling API ----------

    def can_fit(self, cpus, memory, duration):
        if duration < 0:
            return False, None
        if cpus > self.max_cpus or memory > self.max_memory:
            return False, None

        # Empty server → earliest feasible start at t=0 (first job defines creation_time)
        if not self._events:
            return True, 0

        segments = self._build_usage_profile()

        # Establish creation_time if missing (defensive)
        if self.creation_time is None:
            self.creation_time = min(t for (t, _, _) in self._events)

        # --- Dynamic reuse rule ---
        # If longest job on this server (including the candidate) is > reuse_window,
        # there is NO reuse cutoff; otherwise cutoff at creation_time + reuse_window.
        start_lower = self.creation_time
        if self.reuse_window is None:
            start_upper = float('inf')
        else:
            longest_existing = 0
            if self.job_schedule:
                # durations are at index 5 in job_schedule tuples
                longest_existing = max(job[5] for job in self.job_schedule)
            longest_including_this = max(longest_existing, duration)
            start_upper = (float('inf') if longest_including_this > self.reuse_window
                           else self.creation_time + self.reuse_window)

        def segment_can_host(uc, um):
            return (uc + cpus) <= self.max_cpus and (um + memory) <= self.max_memory

        # Candidate starts: creation_time and all segment boundaries, within [start_lower, start_upper]
        candidates = [start_lower] + [s[0] for s in segments]
        candidates = sorted({c for c in candidates if start_lower <= c <= start_upper})

        for start in candidates:
            end = start + duration
            remaining = duration
            t = start
            ok = True

            for (s0, s1, uc, um) in segments:
                if s1 is not None and s1 <= t:
                    continue

                # zero-usage gap before next segment
                if s0 > t:
                    gap_end = min(s0, end)
                    gap_len = gap_end - t
                    # capacity check for the gap is trivial (only this job)
                    if cpus > self.max_cpus or memory > self.max_memory:
                        ok = False
                        break
                    remaining -= gap_len
                    t += gap_len
                    if remaining <= 0:
                        break

                seg_end = end if s1 is None else min(s1, end)
                if t < seg_end:
                    if not segment_can_host(uc, um):
                        ok = False
                        break
                    take = seg_end - t
                    remaining -= take
                    t = seg_end
                    if remaining <= 0:
                        break

            if ok and remaining > 0:
                # tail after last segment (idle)
                if cpus > self.max_cpus or memory > self.max_memory:
                    ok = False
                else:
                    remaining = 0

            if ok and remaining <= 0:
                return True, start

        return False, None


    def add_run(self, run_idx, cpus, memory, pipeline_name, duration=0):
        if self.dedicated and self.runs:
            return False

        can, start_time = self.can_fit(cpus, memory, duration)
        if not can:
            return False
        end_time = start_time + duration

        # first placement defines creation_time
        if not self.job_schedule and self.creation_time is None:
            self.creation_time = start_time

        self._events.append((start_time, +cpus, +memory))
        self._events.append((end_time,   -cpus, -memory))
        self.job_schedule.append((run_idx, pipeline_name, cpus, memory, start_time, duration, end_time))
        self.runs.append((run_idx, pipeline_name))
        return True


    def remove_run(self, run_idx):
        """
        Remove a previously scheduled run and its events.
        Returns True if found and removed; False otherwise.
        """
        # find the job tuple
        pos = None
        for i, job in enumerate(self.job_schedule):
            if job[0] == run_idx:
                pos = i
                break
        if pos is None:
            return False

        run_id, pipeline_name, cpus, memory, start_time, duration, end_time = self.job_schedule.pop(pos)

        # remove from self.runs (first occurrence)
        for i, rp in enumerate(self.runs):
            if rp[0] == run_id:
                self.runs.pop(i)
                break

        # remove the two corresponding events
        # Because events may share timestamps with other jobs, remove by matching both time and deltas.
        removed = 0
        new_events = []
        for (t, dc, dm) in self._events:
            if removed < 2 and (
                (t == start_time and dc == +cpus and dm == +memory) or
                (t == end_time and dc == -cpus and dm == -memory)
            ):
                removed += 1
                continue
            new_events.append((t, dc, dm))
        self._events = new_events
        return True

    def __str__(self):
        peak_c, peak_m = self._peak_usage()
        tag = f"[{self.tag}] "
        return (f"{tag}Server {self.id}: {len(self.runs)} runs, "
                f"peak usage {peak_c}/{self.max_cpus} CPUs, {peak_m:.1f}/{self.max_memory} memory")

def bin_runs_into_servers(runs_df, server_cpus=3, server_memory=16, reuse_window=None):
    """
    Oversize runs (cpus>server_cpus OR mem>server_memory) go to a dedicated 'fargate' server.
    All servers accept new jobs only up to creation_time + reuse_window (if provided).
    """
    servers = []
    run_to_server_mapping = {}

    for idx, row in runs_df.iterrows():
        run_cpus = row['cpus']
        run_memory = row['mem']
        pipeline_name = row['pipeline_name']
        duration = row['duration']

        if duration < 0:
            raise ValueError(f"Run {idx} has negative duration: {duration}")

        # Oversize → dedicated Fargate server sized exactly for the job
        if (run_cpus > server_cpus) or (run_memory > server_memory):
            fargate = Server(
                id=len(servers),
                max_cpus=float(run_cpus),
                max_memory=float(run_memory),
                tag="fargate",
                dedicated=True,
                reuse_window=reuse_window,
            )
            ok = fargate.add_run(idx, run_cpus, run_memory, pipeline_name, duration)
            if not ok:
                raise RuntimeError(f"Oversize run {idx} failed to schedule on its fargate server.")
            servers.append(fargate)
            run_to_server_mapping[idx] = len(servers) - 1
            continue

        # Try to fit on existing EC2 servers (respecting their reuse windows)
        assigned = False
        for i, server in enumerate(servers):
            # print(f"Fitting run {pipeline_name} on server {i} (cpus={run_cpus}, mem={run_memory}, dur={int(duration/60)} minutes)")
            if server.add_run(idx, run_cpus, run_memory, pipeline_name, duration):
                # print(f"Placing {pipeline_name} on server {i}")
                run_to_server_mapping[idx] = i
                assigned = True
                break

        # Otherwise create a new EC2 server
        if not assigned:
            # print(f"Creating new EC2 server for run {pipeline_name} (cpus={run_cpus}, mem={run_memory}, dur={int(duration/60)} minutes)")
            new_server = Server(len(servers), server_cpus, server_memory, tag="ec2", dedicated=False, reuse_window=reuse_window)
            ok = new_server.add_run(idx, run_cpus, run_memory, pipeline_name, duration)
            if not ok:
                raise RuntimeError(f"Run {idx} could not be placed on a fresh EC2 server.")
            servers.append(new_server)
            run_to_server_mapping[idx] = len(servers) - 1

    return servers, run_to_server_mapping


def compact_servers(servers, run_to_server_mapping, runs_df):
    """
    Pack runs onto the earliest possible servers (lowest indices), preserving feasibility.

    Assumptions about Server interface:
      - server.runs: list[tuple[run_id, pipeline_name]]
      - server.job_schedule: list where each entry's first element is run_id
      - server.available_cpus, server.available_memory: numeric resource trackers
      - server.add_run(run_id, cpus, mem, pipeline_name, duration) -> bool
        (Mutates the server when True; no changes when False)
      - server.id: an integer identifier you want to match the position in `servers`

    Notes:
      - Operates on copies; returns new `servers` and `run_to_server_mapping` without
        mutating the passed-in objects.
      - If a run_id is missing in `runs_df`, that run is skipped (not moved).
      - Only moves runs to strictly earlier servers (indices < current index).

    Args:
        servers (list): List of Server objects.
        run_to_server_mapping (dict): Mapping of run_id -> server_index.
        runs_df (pd.DataFrame): Must have index of run_id and columns: 'cpus','mem','duration'.

    Returns:
        tuple[list, dict]: (updated servers list, updated mapping dict)
    """

    # Work on defensive copies so callers don't get surprised by in-place mutations
    servers = deepcopy(servers)
    run_to_server_mapping = deepcopy(run_to_server_mapping)

    def _get_run_resources(rid):
        """Return (cpus, mem, duration) for rid or None if unavailable."""
        # Use .at for scalar lookups when index is exact; fall back to .loc if needed
        try:
            cpus = runs_df.at[rid, 'cpus']
            mem = runs_df.at[rid, 'mem']
            dur = runs_df.at[rid, 'duration']
            return cpus, mem, dur
        except KeyError:
            try:
                row = runs_df.loc[rid]
                return row['cpus'], row['mem'], row['duration']
            except Exception:
                return None

    # Start from the last server and move backward
    server_idx = len(servers) - 1

    while server_idx > 0:  # server 0 is the earliest; we never try to move into a later server
        # Defensive: in case the list changed unexpectedly
        if server_idx >= len(servers):
            server_idx = len(servers) - 1
            if server_idx <= 0:
                break

        current_server = servers[server_idx]

        # Iterate runs from the end toward the start; if we remove an item,
        # we simply decrement the index and continue (no brittle equal-length checks).
        run_idx = len(current_server.runs) - 1

        while run_idx >= 0:
            try:
                run_id, pipeline_name = current_server.runs[run_idx]
            except (ValueError, IndexError):
                # Malformed entry; skip it safely
                run_idx -= 1
                continue

            res = _get_run_resources(run_id)
            if res is None:
                # Cannot determine resources; skip moving this run
                run_idx -= 1
                continue

            run_cpus, run_memory, run_duration = res

            relocated = False

            # Try to place on the earliest feasible server [0 .. server_idx-1]
            for target_idx in range(server_idx):
                target_server = servers[target_idx]

                # Attempt to add; if it fits, target_server mutates to include the run
                if target_server.add_run(run_id, run_cpus, run_memory, pipeline_name, run_duration):
                    # Update mapping
                    run_to_server_mapping[run_id] = target_idx

                    # Remove from the current server (events + schedule + runs)
                    current_server.remove_run(run_id)

                    relocated = True
                    break  # stop searching target servers for this run

            # Move to previous run; if relocated, we already popped the current index
            run_idx -= 1

        # If the server is empty after relocations, remove it and shift mappings
        if not getattr(current_server, 'runs', None):
            # Remove the server at server_idx
            servers.pop(server_idx)

            # Any runs mapped to servers with index > server_idx now shift left by 1
            # Iterate over a list of items to avoid iterator invalidation concerns
            for rid, sid in list(run_to_server_mapping.items()):
                if sid > server_idx:
                    run_to_server_mapping[rid] = sid - 1

        # Move to the previous server index
        server_idx -= 1

    # Normalize server IDs to match their positions
    for i, server in enumerate(servers):
        if hasattr(server, 'id'):
            server.id = i

    return servers, run_to_server_mapping

def find_max_concurrent_jobs(servers, skip_tags=None):
    """
    Calculate the maximum number of concurrent jobs across all servers,
    optionally skipping servers with certain tags (e.g., {'fargate'}).

    Args:
        servers (list): List of Server objects
        skip_tags (set|None): e.g., {'fargate'} to ignore fargate servers

    Returns:
        dict with:
            - 'max_concurrent_jobs'
            - 'peak_time'
            - 'timeline': [(time, job_count), ...]
    """
    skip_tags = set(skip_tags or [])
    job_events = []

    for server in servers:
        tag = getattr(server, 'tag', None)
        if tag in skip_tags:
            continue
        for job in server.job_schedule:
            _, _, _, _, start_time, _, end_time = job
            job_events.append((start_time, 1))   # start = +1
            job_events.append((end_time, -1))    # end   = -1

    if not job_events:
        return {'max_concurrent_jobs': 0, 'peak_time': 0, 'timeline': []}

    # Sort ensures (time, -1) comes before (time, +1) at the same timestamp
    job_events.sort()

    current = 0
    peak = 0
    peak_t = 0
    timeline = []
    for t, delta in job_events:
        current += delta
        timeline.append((t, current))
        if current > peak:
            peak = current
            peak_t = t

    return {
        'max_concurrent_jobs': peak,
        'peak_time': peak_t,
        'timeline': timeline
    }

def count_servers(servers, tag):
    return len([s for s in servers if s.tag == tag])

def server_time_metrics(srv):
    """
    Returns:
        span (float): last_end - first_start (0 if no jobs)
        busy (float): sum of intervals with >=1 running jobs
        first_start (float|None)
        last_end (float|None)
    """
    if not srv.job_schedule:
        return 0.0, 0.0, None, None

    starts = [j[4] for j in srv.job_schedule]  # start_time
    ends   = [j[6] for j in srv.job_schedule]  # end_time
    first_start, last_end = min(starts), max(ends)
    span = last_end - first_start

    # Build start/end events and sweep; ensure ends come before starts at same t
    events = []
    for _, _, _, _, s, _, e in srv.job_schedule:
        events.append((s, +1))
        events.append((e, -1))
    events.sort()  # (t, -1) sorts before (t, +1)

    busy = 0.0
    current = 0
    prev_t = events[0][0]
    for t, delta in events:
        if current > 0:
            busy += (t - prev_t)
        current += delta
        prev_t = t

    return span, busy, first_start, last_end

def server_peak_usage(srv):
        events = []
        for run_id, pipeline_name, cpus, mem, start, dur, end in srv.job_schedule:
            events.append((start, +cpus, +mem))
            events.append((end,   -cpus, -mem))
        events.sort(key=lambda e: e[0])

        used_c, used_m = 0, 0
        peak_c, peak_m = 0, 0
        for _, dc, dm in events:
            used_c += dc
            used_m += dm
            peak_c = max(peak_c, used_c)
            peak_m = max(peak_m, used_m)
        return peak_c, peak_m

def main():
    # ---- Paths (adjust as needed) ----
    txt_file_path = '..'
    csv_file_path = '..'

    # ---- Read & prep inputs ----
    pipeline_names = read_pipeline_names(txt_file_path)
    pipeline_names_df = pd.DataFrame({'pipeline_name': pipeline_names})

    print(f"Pipeline names DataFrame created successfully with {len(pipeline_names_df)} rows")

    csv_df = load_csv_file(csv_file_path)

    # Keep only runs whose pipeline_name is in mid.txt
    # runs_df = csv_df[csv_df['pipeline_name'].isin(pipeline_names_df['pipeline_name'])].copy()
    runs_df = csv_df
    print(f"Filtered runs DataFrame created with {len(runs_df)} rows out of {len(csv_df)} original rows")

    # Clean/ensure types
    runs_df['mem'] = runs_df['mem'].astype(str).str.replace('Gi', '', regex=False).astype(float)
    runs_df['cpus'] = pd.to_numeric(runs_df['cpus'], errors='coerce')
    runs_df['duration'] = pd.to_numeric(runs_df['duration'], errors='coerce')

    # Drop rows with missing essential fields
    runs_df = runs_df.dropna(subset=['cpus', 'mem', 'duration'])

    total_duration = runs_df['duration'].sum()
    total_memory = runs_df['mem'].sum()
    total_cpus = runs_df['cpus'].sum()
    print(f"Total duration across {len(runs_df)} runs: {total_duration}")
    print(f"Total memory across {len(runs_df)} runs: {total_memory}")
    print(f"Total CPUs across {len(runs_df)} runs: {total_cpus}")
    
    max_ec2 = 0
    max_fargate = 0
    max_concurrent = 0

    for n in range(1000):

        # runs_df = runs_df.sort_values(by='duration', ascending=False)
        runs_df = runs_df.sample(frac=1, random_state=int(time.time()))
    
        reuse_window = 8 * 60 * 60
    
        servers, run_to_server_mapping = bin_runs_into_servers(
            runs_df, server_cpus=3, server_memory=16, reuse_window=reuse_window
        )
    
        # Map server assignment back to DataFrame index (run_id)
        runs_df['server_id'] = runs_df.index.map(lambda idx: run_to_server_mapping.get(idx, -1))
        
        ec2_count = count_servers(servers, "ec2")
        fargate_count = count_servers(servers, "fargate")
        concurrency_info = find_max_concurrent_jobs(servers, skip_tags={'fargate'})
    
        concurrent = concurrency_info['max_concurrent_jobs']

        max_ec2 = max(max_ec2, ec2_count)
        max_fargate = max(max_fargate, fargate_count)
        max_concurrent = max(max_concurrent, concurrent)

        print(f"{n}: ec2: {max_ec2}, fargate: {max_fargate}, concurrent: {max_concurrent}")

    print()
    print(f"Overall max EC2 servers across runs: {max_ec2}")
    print(f"Overall max Fargate servers across runs: {max_fargate}")
    print(f"Overall max concurrent jobs (EC2 only) across runs: {max_concurrent}")

    
if __name__ == '__main__':
    main()
