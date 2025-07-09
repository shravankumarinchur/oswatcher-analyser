import os
import re
import gzip
import shutil
import contextlib

def get_oswarchive_path():
    while True:
        path = input("Enter the absolute path to the OSWatcher archive directory: ").strip()
        if os.path.isdir(path):
            return path
        else:
            print("Invalid directory path. Please try again.")

def unzip_gz_files(directory, silent=True):
    try:
        files = os.listdir(directory)
    except Exception as e:
        if not silent:
            print(f"⚠️ Error accessing directory '{directory}': {e}")
        return False  # Signal failure to calling function

    success = True
    for file in files:
        if file.endswith(".gz"):
            gz_path = os.path.join(directory, file)
            dat_path = gz_path[:-3]
            if not os.path.exists(dat_path):
                if not silent:
                    print(f"Unzipping: {file}")
                try:
                    with gzip.open(gz_path, 'rb') as f_in:
                        with open(dat_path, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                    os.remove(gz_path)
                except Exception as e:
                    print(f"❌ Error unzipping {file}: {e}")
                    success = False  # One file failed, still keep going

    return success  # True if all/unzipped or already extracted, False if any error occurred


def get_cpu_cores_from_vmstat(vmstat_dir):
    #unzip_gz_files(vmstat_dir)
    for file in sorted(os.listdir(vmstat_dir)):
        if file.endswith(".dat"):
            file_path = os.path.join(vmstat_dir, file)
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    if line.startswith("VCPUS"):
                        try:
                            cores = int(line.strip().split()[1])
                            print(f"\n🧠 Detected CPU Cores (VCPUS): {cores}")
                            return cores
                        except (IndexError, ValueError):
                            pass
    print("Could not determine CPU cores from vmstat data.")
    exit(1)

def extract_date_from_filename(filename):
    match = re.search(r"_(\d{2}\.\d{2}\.\d{2})\.\d{4}\.dat$", filename)
    return match.group(1) if match else "Unknown Date"

def detect_increasing_load_patterns(load_data, cpu_cores, min_consecutive=6):
    threshold_50 = 0.5 * cpu_cores
    increasing_patterns = []
    temp_pattern = []
    tracking = False

    for i in range(1, len(load_data)):
        prev_time, prev_date, prev_load_1m, _, _ = load_data[i - 1]
        curr_time, curr_date, curr_load_1m, _, _ = load_data[i]

        if curr_load_1m > threshold_50:
            if not tracking and prev_load_1m > threshold_50:
                tracking = True
                temp_pattern = [(prev_time, prev_date, prev_load_1m)]

            if curr_load_1m > prev_load_1m:
                temp_pattern.append((curr_time, curr_date, curr_load_1m))
            else:
                if len(temp_pattern) >= min_consecutive:
                    increasing_patterns.append(temp_pattern)
                temp_pattern = []
                tracking = False
        else:
            tracking = False
            temp_pattern = []

    if len(temp_pattern) >= min_consecutive:
        increasing_patterns.append(temp_pattern)

    if increasing_patterns:
        print("\n=== 📈 Detected Increasing Load Average Patterns (5+ consecutive) ===")
        for pattern in increasing_patterns:
            print("📈 Pattern Detected:")
            for time_val, date_val, load in pattern:
                print(f"  [{date_val} {time_val}] Load: {load:.2f}")
            print("-" * 40)

def detect_decreasing_load_patterns(load_data, cpu_cores, min_consecutive=6):
    threshold_75 = 0.75 * cpu_cores
    decreasing_patterns = []
    temp_pattern = []
    tracking = False

    for i in range(len(load_data) - 1):
        curr_time, curr_date, curr_load_1m, _, _ = load_data[i]
        next_time, next_date, next_load_1m, _, _ = load_data[i + 1]

        if not tracking:
            if curr_load_1m > threshold_75:
                tracking = True
                temp_pattern = [(curr_time, curr_date, curr_load_1m)]
        else:
            if next_load_1m < curr_load_1m:
                temp_pattern.append((next_time, next_date, next_load_1m))
            else:
                if len(temp_pattern) >= min_consecutive:
                    decreasing_patterns.append(temp_pattern)
                temp_pattern = []
                tracking = False

    if len(temp_pattern) >= min_consecutive:
        decreasing_patterns.append(temp_pattern)

    if decreasing_patterns:
        print("\n=== 📉 Detected Decreasing Load Average Patterns (6+ consecutive) ===")
        for pattern in decreasing_patterns:
            print("📉 Decreasing Pattern Detected:")
            for time_val, date_val, load in pattern:
                print(f"  [{date_val} {time_val}] Load: {load:.2f}")
            print("-" * 40)
    else:
        print("\n✅ No significant decreasing load average patterns detected.")

def process_oswtop_files(directory, cpu_cores, threshold_75):
    pattern = re.compile(r"^top - (\d{2}:\d{2}:\d{2}) .*load average: ([\d.]+), ([\d.]+), ([\d.]+)")

    highest = None
    lowest = None
    load_data = []

    print(f"\n========📊 Analyzing Server instances where CPU crossed 75%+ usage=============\n")
    
    print(f"\n The total cpu cores : {cpu_cores}\n")
    for filename in sorted(os.listdir(directory)):
        if filename.endswith(".dat"):
            filepath = os.path.join(directory, filename)
            date = extract_date_from_filename(filename)

            with open(filepath, "r", encoding="utf-8", errors="ignore") as file:
                for line in file:
                    match = pattern.search(line)
                    if match:
                        timestamp, load_avg_1, load_avg_5, load_avg_15 = match.groups()
                        load_avg_1 = float(load_avg_1)
                        load_avg_5 = float(load_avg_5)
                        load_avg_15 = float(load_avg_15)
                        load_data.append((timestamp, date, load_avg_1, load_avg_5, load_avg_15))

                        if load_avg_1 > threshold_75:
                            print(f"{filename} - {timestamp} | Load Avg (1m: {load_avg_1}, 5m: {load_avg_5}, 15m: {load_avg_15})")

                        if highest is None or load_avg_1 > highest[0]:
                            highest = (load_avg_1, timestamp, date, filename)
                        if lowest is None or load_avg_1 < lowest[0]:
                            lowest = (load_avg_1, timestamp, date, filename)

    if highest:
        print(f"\n=======🔺 Peak Load Summary 🔺 =======\n"
              f"Filename: {highest[3]}\nDate: {highest[2]}\nTime: {highest[1]}\nPeak Load Avg: {highest[0]}\n")

    if lowest:
        print(f"\n======= 🔻 Lowest Load Summary 🔻 =======\n"
              f"Filename: {lowest[3]}\nDate: {lowest[2]}\nTime: {lowest[1]}\nLowest Load Avg: {lowest[0]}\n")

    detect_increasing_load_patterns(load_data, cpu_cores, min_consecutive=6)
    detect_decreasing_load_patterns(load_data, cpu_cores, min_consecutive=6)

def detect_increasing_memory_patterns(mem_data, min_consecutive=6):
    pattern = []
    tracking = False
    increasing_patterns = []

    for i in range(1, len(mem_data)):
        prev = mem_data[i - 1]
        curr = mem_data[i]

        if curr[1] > 50:
            if not tracking and prev[1] > 50:
                tracking = True
                pattern = [prev]

            if curr[1] > prev[1]:
                pattern.append(curr)
            else:
                if len(pattern) >= min_consecutive:
                    increasing_patterns.append(pattern)
                pattern = []
                tracking = False
        else:
            pattern = []
            tracking = False

    if len(pattern) >= min_consecutive:
        increasing_patterns.append(pattern)

    if increasing_patterns:
        print("\n=== 📈 Detected Increasing Memory Usage Patterns (5+ consecutive) ===")
        for p in increasing_patterns:
            print("📈 Pattern Detected:")
            for entry in p:
                ts, used_pct, used_gb, free_gb = entry
                print(f"  [{ts}] Used: {used_pct:.2f}% ({used_gb:.2f} GB), Free: {free_gb:.2f} GB")
            print("-" * 40)

def detect_decreasing_memory_patterns(mem_data, min_consecutive=6):
    pattern = []
    tracking = False
    decreasing_patterns = []

    for i in range(len(mem_data) - 1):
        curr = mem_data[i]
        next_ = mem_data[i + 1]

        if not tracking:
            if curr[1] > 75:
                tracking = True
                pattern = [curr]
        else:
            if next_[1] < curr[1]:
                pattern.append(next_)
            else:
                if len(pattern) >= min_consecutive:
                    decreasing_patterns.append(pattern)
                pattern = []
                tracking = False

    if len(pattern) >= min_consecutive:
        decreasing_patterns.append(pattern)

    if decreasing_patterns:
        print("\n=== 📉 Detected Decreasing Memory Usage Patterns (6+ consecutive) ===")
        for p in decreasing_patterns:
            print("📉 Pattern Detected:")
            for entry in p:
                ts, used_pct, used_gb, free_gb = entry
                print(f"  [{ts}] Used: {used_pct:.2f}% ({used_gb:.2f} GB), Free: {free_gb:.2f} GB")
            print("-" * 40)
    else:
        print("\n✅ No significant decreasing memory usage patterns detected.")

def process_oswmeminfo_files(meminfo_dir):
    print("\n========🧠 Analyzing Memory Usage above 75%=========\n")
    #unzip_gz_files(meminfo_dir)

    highest = None
    lowest = None
    total_gb = None
    printed_total = False
    mem_data = []

    for filename in sorted(os.listdir(meminfo_dir)):
        if filename.endswith(".dat"):
            filepath = os.path.join(meminfo_dir, filename)
            with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                timestamp = None
                values = {}

                for line in f:
                    if line.startswith("zzz "):
                        if values:
                            try:
                                total = int(values["MemTotal"])
                                free = int(values["MemFree"])
                                buffers = int(values["Buffers"])
                                cached = int(values["Cached"])

                                free_mem_kb = free + buffers + cached
                                used_mem_kb = total - free_mem_kb
                                used_pct = (used_mem_kb / total) * 100
                                free_pct = 100 - used_pct

                                total_gb = total / (1024 * 1024)
                                used_gb = used_mem_kb / (1024 * 1024)
                                free_gb = free_mem_kb / (1024 * 1024)

                                if not printed_total:
                                    print(f"💾 Total Memory on Server: {total_gb:.2f} GB\n")
                                    printed_total = True

                                if used_pct > 75:
                                    print(f"{timestamp} | Used: {used_pct:.2f}% ({used_gb:.2f} GB), Free: {free_pct:.2f}% ({free_gb:.2f} GB)")

                                mem_data.append((timestamp, used_pct, used_gb, free_gb))

                                if highest is None or used_pct > highest[0]:
                                    highest = (used_pct, timestamp, used_gb, free_gb)
                                if lowest is None or used_pct < lowest[0]:
                                    lowest = (used_pct, timestamp, used_gb, free_gb)

                            except KeyError:
                                pass

                        values = {}
                        timestamp = line.strip().replace("zzz ", "").replace("***", "")

                    else:
                        parts = line.split()
                        if len(parts) >= 2 and parts[0].endswith(":"):
                            key = parts[0].rstrip(":")
                            val = parts[1]
                            if key in ["MemTotal", "MemFree", "Buffers", "Cached"]:
                                values[key] = val

    if highest:
        print(f"\n======= 🔺 Peak Memory Usage Summary 🔺 =======")
        print(f"Timestamp: {highest[1]}")
        print(f"Used: {highest[0]:.2f}% ({highest[2]:.2f} GB), Free: {100 - highest[0]:.2f}% ({highest[3]:.2f} GB)")

    if lowest:
        print(f"\n======= 🔻 Lowest Memory Usage Summary 🔻 =======")
        print(f"Timestamp: {lowest[1]}")
        print(f"Used: {lowest[0]:.2f}% ({lowest[2]:.2f} GB), Free: {100 - lowest[0]:.2f}% ({lowest[3]:.2f} GB)")

    detect_increasing_memory_patterns(mem_data, min_consecutive=6)
    detect_decreasing_memory_patterns(mem_data, min_consecutive=6)

def process_oswvmstat_files(vmstat_dir, cpu_cores):
    print("\n========⚙️ Analyzing vmstat output where 'r' > CPU cores ========\n")

    r_exceeds = []

    for filename in sorted(os.listdir(vmstat_dir)):
        if filename.endswith(".dat"):
            filepath = os.path.join(vmstat_dir, filename)
            timestamp = None
            with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    if line.startswith("zzz "):
                        timestamp = line.strip().replace("zzz ", "").replace("***", "")
                    elif re.match(r"\s*\d+", line):
                        columns = re.split(r"\s+", line.strip())
                        if len(columns) >= 6:
                            try:
                                r_val = int(columns[0])
                                b_val = int(columns[1])
                                if r_val > cpu_cores:
                                    r_exceeds.append((timestamp, r_val, b_val))
                            except ValueError:
                                continue

    if r_exceeds:
        print("⚠️  Detected times where 'r' (running processes) > CPU cores:\n")
        for ts, r, b in r_exceeds:
            print(f"  [{ts}] r = {r}, b = {b}")
        print(f"\n🔍 Total occurrences: {len(r_exceeds)}")
    else:
        print("✅ No 'r' values exceeding CPU cores detected.")





def analyze_oswtop_data(oswtop_dir):
    
    timestamp_header_pattern = re.compile(r'^zzz \*\*\*(.*?)$')

    # Pattern to match process lines
    process_line_pattern = re.compile(
        r'^\s*(\d+)\s+(\S+)\s+\d+\s+\S+\s+\S+\s+\S+\s+\S+\s+([RSDZTW])\s+([\d.]+)\s+([\d.]+)\s+[\d:.]+\s+(.+)$'
    )

    for root, _, files in os.walk(oswtop_dir):
        for file in sorted(files):
            filepath = os.path.join(root, file)
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()

            current_timestamp = None
            process_list = []

            for line in lines:
                line = line.strip()

                match_ts = timestamp_header_pattern.match(line)
                if match_ts:
                    if current_timestamp and process_list:
                        d_processes = [proc for proc in process_list if proc['state'] == 'D']
                        if d_processes:
                            print(f"\n[{current_timestamp}] D-state Processes (Count: {len(d_processes)}):")
                            for proc in d_processes:
                                print(f"PID={proc['pid']}, USER={proc['user']}, STATE={proc['state']}, CPU={proc['cpu']}%, MEM={proc['mem']}%, CMD={proc['cmd']}")
                    current_timestamp = match_ts.group(1)
                    process_list = []
                    continue

                match_proc = process_line_pattern.match(line)
                if match_proc:
                    proc_info = {
                        'pid': match_proc.group(1),
                        'user': match_proc.group(2),
                        'state': match_proc.group(3),
                        'cpu': float(match_proc.group(4)),
                        'mem': float(match_proc.group(5)),
                        'cmd': match_proc.group(6)
                    }
                    process_list.append(proc_info)

            # Final block for last timestamp
            if current_timestamp and process_list:
                d_processes = [proc for proc in process_list if proc['state'] == 'D']
                if d_processes:
                    print(f"\n[{current_timestamp}] D-state Processes (Count: {len(d_processes)}):")
                    for proc in d_processes:
                        print(f"PID={proc['pid']}, USER={proc['user']}, STATE={proc['state']}, CPU={proc['cpu']}%, MEM={proc['mem']}%, CMD={proc['cmd']}")







def analyze_iostat_files(directory):
    iowait_records = []  # To store tuples (timestamp, iowait)
    high_util_disks = []  # To store tuples (timestamp, disk, read_MBps, write_MBps, util%)

    def kb_to_mb(kb):
        return kb / 1024.0

    for filename in sorted(os.listdir(directory)):
        if not filename.endswith(".dat"):
            continue

        filepath = os.path.join(directory, filename)
        with open(filepath, 'r') as f:
            timestamp = None
            for line in f:
                line = line.strip()

                # Extract timestamp
                if line.startswith('zzz') or line.startswith('***'):
                    timestamp = line.split('***')[-1].strip()
                    continue

                # Handle avg-cpu section
                if line.startswith('avg-cpu:'):
                    try:
                        cpu_line = next(f).strip()
                        parts = cpu_line.split()
                        if len(parts) >= 4:
                            iowait = float(parts[3])
                            iowait_records.append((timestamp, iowait))
                    except StopIteration:
                        continue
                    continue

                if line.startswith('Device') or line == '':
                    continue

                parts = line.split()
                if len(parts) < 14:
                    continue

                try:
                    device = parts[0]
                    read_kBps = float(parts[5])   # Corrected index
                    write_kBps = float(parts[6])  # Corrected index
                    util = float(parts[-1])
                except ValueError:
                    continue

                if util > 50.0:
                    read_MBps = kb_to_mb(read_kBps)
                    write_MBps = kb_to_mb(write_kBps)
                    high_util_disks.append((timestamp, device, read_MBps, write_MBps, util))

    # Print top 10 iowait values
    print("Top 10 highest iowait values:")
    for ts, io in sorted(iowait_records, key=lambda x: x[1], reverse=True)[:10]:
        print(f"{ts} - iowait: {io:.2f}%")

    # Print high-utilization disks
    print("\nDisks with utilization > 50%:")
    for ts, dev, r_mb, w_mb, util in high_util_disks:
        print(f"{ts} - Device: {dev}, Read: {r_mb:.2f} MB/s, Write: {w_mb:.2f} MB/s, Utilization: {util:.2f}%")
#analyze_iostat_files("oswiostat")
if __name__ == "__main__":
    archive_dir = get_oswarchive_path()

    oswtop_dir = os.path.join(archive_dir, "oswtop")
    oswvmstat_dir = os.path.join(archive_dir, "oswvmstat")
    oswmeminfo_dir = os.path.join(archive_dir, "oswmeminfo")
    oswiostat_dir= os.path.join(archive_dir, "oswiostat")

    
   #unzip_gz_files(oswvmstat_dir)
   #unzip_gz_files(oswmeminfo_dir)
   # unzip_gz_files(oswiostat_dir)

    
    while True:
        print("\n========== OSWatcher Analysis Menu ==========")
        print("1. Check CPU performance only")
        print("2. Check Memory performance only")
        print("3. Check vmstat")
        print("4. Analyze D-state and High CPU/Memory Processes")
        print("5. Analyze Disk and IOwait")
        print("6. Exit")
        choice = input("Enter your choice (1-6): ").strip()

        if choice == "1":
            if not unzip_gz_files(oswtop_dir, silent=False):
              print("❌ Failed to unzip files in oswtop_dir. Returning to main menu.")
              continue 
            if not unzip_gz_files(oswvmstat_dir, silent=False):
              print("❌ Failed to unzip files in oswvmstat_dir. Returning to main menu.")
              continue
            output_path = os.path.join(archive_dir, "cpu_analysis.txt")
            cpu_cores = get_cpu_cores_from_vmstat(oswvmstat_dir)
            threshold_75 = 0.75 * cpu_cores
            with open(output_path, "w") as f, contextlib.redirect_stdout(f):
                process_oswtop_files(oswtop_dir, cpu_cores, threshold_75)
            print(f"✅ CPU analysis written to: {output_path}")

        elif choice == "2":
            if not unzip_gz_files(oswmeminfo_dir, silent=False):
             print("❌ Failed to unzip files in oswmeminfo_dir. Returning to main menu.")
             continue
            output_path = os.path.join(archive_dir, "memory_analysis.txt")
            with open(output_path, "w") as f, contextlib.redirect_stdout(f):
                process_oswmeminfo_files(oswmeminfo_dir)
            print(f"✅ Memory analysis written to: {output_path}")

        elif choice == "3":
            if not unzip_gz_files(oswvmstat_dir, silent=False):
             print("❌ Failed to unzip files in oswvmstat_dir. Returning to main menu.")
             continue
            output_path = os.path.join(archive_dir, "vmstat_analysis.txt")
            cpu_cores = get_cpu_cores_from_vmstat(oswvmstat_dir)
            with open(output_path, "w") as f, contextlib.redirect_stdout(f):
                process_oswvmstat_files(oswvmstat_dir, cpu_cores)
            print(f"✅ vmstat analysis written to: {output_path}")

        elif choice == "4":
            if not unzip_gz_files(oswtop_dir, silent=False):
             print("❌ Failed to unzip files in oswvmstat_dir. Returning to main menu.")
             continue
            output_path = os.path.join(archive_dir, "dstate_and_high_resource_processes.txt")
            with open(output_path, "w") as f, contextlib.redirect_stdout(f):
                analyze_oswtop_data(oswtop_dir)
            print(f"✅ D-state and High Resource Process analysis written to: {output_path}")
        
        elif choice == "5":
            if not unzip_gz_files(oswiostat_dir, silent=False):
             print("❌ Failed to unzip files in oswvmstat_dir. Returning to main menu.")
             continue 
            output_path = os.path.join(archive_dir, "disk and iowait details.txt")
            with open(output_path, "w") as f, contextlib.redirect_stdout(f):
                analyze_iostat_files(oswiostat_dir)
            print(f"✅ DIOwait and disk analysis written to: {output_path}")

        elif choice == "6":
            print("✅ Exiting. Goodbye Shravan!")
            break

        else:
            print("❌ Invalid choice. Try again.")
