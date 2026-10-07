#!/usr/bin/env bash
# Sample the streamer's CPU, memory, threads and RTSP clients over time, so
# a run with several cameras can be compared idle against live and a
# leak shows up as a slope (threads or RSS that only grow).
#
# Reads /proc only: no root needed for a container started by your own
# user or the docker group, nothing installed in the image. Writes one CSV
# row per sample and prints a summary per number of live cameras.
#
# Usage:
#   scripts/measure.sh [-c CONTAINER | -p PID] [-i SECONDS] [-d SECONDS]
#                      [-m METRICS_URL] [-o FILE.csv]
#
#   -c  container name or id (default: arlo-camera-streamer); the engine
#       is $CONTAINER_ENGINE (default: docker, podman works too)
#   -p  PID of a daemon run outside a container
#   -i  seconds between samples (default: 5)
#   -d  seconds to run (default: 600); Ctrl-C stops early and still
#       prints the summary
#   -m  the daemon's /metrics URL, for the idle/live count per sample
#       (default: http://127.0.0.1:9090/metrics). Reaching it from the host
#       needs metrics_bind = "0.0.0.0:9090" in the config and the port
#       published; without it the camera columns stay empty.
#   -o  CSV output (default: measure-<UTC timestamp>.csv)
#
# Columns: utc, elapsed_s, cpu_pct (100 = one core), rss_mib, threads,
# rtsp_clients (established TCP connections to the RTSP port), live,
# activating, idle (cameras in each state, from /metrics).

set -euo pipefail

readonly DEFAULT_CONTAINER="arlo-camera-streamer"
readonly DEFAULT_INTERVAL_S=5
readonly DEFAULT_DURATION_S=600
readonly DEFAULT_METRICS_URL="http://127.0.0.1:9090/metrics"
readonly RTSP_PORT=8554
# /proc/net/tcp state code for an established connection.
readonly TCP_ESTABLISHED="01"
readonly METRICS_TIMEOUT_S=2
readonly KIB_PER_MIB=1024
readonly NS_PER_S=1000000000

container="$DEFAULT_CONTAINER"
pid=""
interval="$DEFAULT_INTERVAL_S"
duration="$DEFAULT_DURATION_S"
metrics_url="$DEFAULT_METRICS_URL"
out=""

usage() {
    sed -n '/^# Usage:/,/^# Columns:/p' "$0" | sed 's/^# \{0,1\}//' >&2
    exit 2
}

die() {
    echo "measure: $*" >&2
    exit 1
}

while getopts "c:p:i:d:m:o:h" opt; do
    case "$opt" in
        c) container="$OPTARG" ;;
        p) pid="$OPTARG" ;;
        i) interval="$OPTARG" ;;
        d) duration="$OPTARG" ;;
        m) metrics_url="$OPTARG" ;;
        o) out="$OPTARG" ;;
        *) usage ;;
    esac
done

[[ "$interval" =~ ^[1-9][0-9]*$ ]] || die "-i must be a whole number of seconds"
[[ "$duration" =~ ^[1-9][0-9]*$ ]] || die "-d must be a whole number of seconds"
out="${out:-measure-$(date -u +%Y%m%dT%H%M%SZ).csv}"

# The daemon's PID. In the image, PID 1 of the container is tini and the
# daemon is its only child.
resolve_pid() {
    if [[ -n "$pid" ]]; then
        [[ "$pid" =~ ^[0-9]+$ ]] || die "-p must be a PID"
        echo "$pid"
        return
    fi
    local engine="${CONTAINER_ENGINE:-docker}" init children
    init=$("$engine" inspect -f '{{.State.Pid}}' "$container" 2>/dev/null) ||
        die "$engine cannot inspect container '$container' (is it running?)"
    [[ "$init" =~ ^[1-9][0-9]*$ ]] || die "container '$container' is not running"
    children=$(cat "/proc/$init/task/$init/children" 2>/dev/null || true)
    if [[ "$(cat "/proc/$init/comm")" == "tini" && -n "$children" ]]; then
        echo "${children%% *}"
    else
        echo "$init"
    fi
}

target=$(resolve_pid)
[[ -r "/proc/$target/stat" ]] || die "cannot read /proc/$target"
clk_tck=$(getconf CLK_TCK)
readonly target clk_tck

# utime + stime of the process (all its threads), in clock ticks. The
# command name in field 2 may hold spaces, so fields are counted after
# its closing parenthesis.
cpu_ticks() {
    local stat
    stat=$(<"/proc/$target/stat")
    awk '{ print $12 + $13 }' <<<"${stat##*) }"
}

status_field() {
    awk -v key="$1:" '$1 == key { print $2 }' "/proc/$target/status"
}

# Established connections to the RTSP port, in the daemon's network
# namespace (so a container's own sockets, not the host's).
rtsp_clients() {
    local port_hex
    port_hex=$(printf '%04X' "$RTSP_PORT")
    cat "/proc/$target/net/tcp" "/proc/$target/net/tcp6" 2>/dev/null |
        awk -v port=":$port_hex" -v est="$TCP_ESTABLISHED" '
            NR > 1 && $4 == est && substr($2, length($2) - 4) == port { n++ }
            END { print n + 0 }'
}

# "live,activating,idle" from streamer_camera_state, or ",," when the
# endpoint cannot be read.
camera_states() {
    local body
    if ! body=$(curl -fsS --max-time "$METRICS_TIMEOUT_S" "$metrics_url" 2>/dev/null); then
        echo ",,"
        return
    fi
    awk '
        /^streamer_camera_state\{/ && $NF == 1 {
            match($0, /state="[^"]*"/)
            count[substr($0, RSTART + 7, RLENGTH - 8)]++
        }
        END { printf "%d,%d,%d\n", count["live"], count["activating"], count["idle"] }
    ' <<<"$body"
}

summarise() {
    echo
    echo "Samples: $out"
    awk -F, '
        NR == 1 { next }
        {
            key = ($7 == "") ? "unknown" : $7
            n[key]++; cpu[key] += $3; rss[key] += $4
            if ($3 > cpu_max[key]) cpu_max[key] = $3
            if ($4 > rss_max[key]) rss_max[key] = $4
            if (first_t == "") { first_t = $5; first_rss = $4; first_s = $2 }
            last_t = $5; last_rss = $4; last_s = $2
        }
        END {
            if (first_t == "") { print "no samples"; exit }
            printf "%-12s %8s %10s %10s %11s %11s\n", "live cams", "samples", "cpu% avg", "cpu% max", "RSS MiB avg", "RSS MiB max"
            for (k in n)
                printf "%-12s %8d %10.1f %10.1f %11.1f %11.1f\n", k, n[k], cpu[k] / n[k], cpu_max[k], rss[k] / n[k], rss_max[k]
            printf "Over %d s: threads %d -> %d, RSS %.1f -> %.1f MiB\n", last_s - first_s, first_t, last_t, first_rss, last_rss
        }
    ' "$out"
}

echo "utc,elapsed_s,cpu_pct,rss_mib,threads,rtsp_clients,live,activating,idle" >"$out"
echo "measure: PID $target ($(cat "/proc/$target/comm")), every ${interval}s for ${duration}s -> $out" >&2
trap 'summarise; exit 0' INT TERM

start_ns=$(date +%s%N)
prev_ticks=$(cpu_ticks)
prev_ns=$start_ns
while :; do
    sleep "$interval"
    [[ -r "/proc/$target/stat" ]] || { echo "measure: PID $target exited" >&2; break; }
    now_ns=$(date +%s%N)
    ticks=$(cpu_ticks)
    cpu=$(awk -v d="$((ticks - prev_ticks))" -v hz="$clk_tck" -v ns="$((now_ns - prev_ns))" -v per="$NS_PER_S" \
        'BEGIN { printf "%.1f", (ns > 0) ? d / hz / (ns / per) * 100 : 0 }')
    elapsed=$(((now_ns - start_ns) / NS_PER_S))
    rss=$(awk -v kib="$(status_field VmRSS)" -v per="$KIB_PER_MIB" 'BEGIN { printf "%.1f", kib / per }')
    printf '%s,%d,%s,%s,%s,%s,%s\n' \
        "$(date -u +%FT%TZ)" "$elapsed" "$cpu" "$rss" \
        "$(status_field Threads)" "$(rtsp_clients)" "$(camera_states)" >>"$out"
    prev_ticks=$ticks
    prev_ns=$now_ns
    (( elapsed >= duration )) && break
done
summarise
