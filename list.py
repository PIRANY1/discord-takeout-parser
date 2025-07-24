# script2_discord_event_analyzer.py

import json
import csv
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Tuple
from collections import defaultdict, Counter

# --- EVENT CONFIGURATION ---
CALL_START_EVENTS = {"join_call", "start_call", "voice_connection_success"}
CALL_END_EVENTS   = {"voice_disconnect"}

STREAM_START_EVENT = "video_stream_started"
STREAM_END_EVENT   = "video_stream_ended"  # Only counted for completeness

MESSAGE_EVENT = "send_message"
# --- END CONFIGURATION ---


def format_duration(seconds: float) -> str:
    """Convert seconds to 'Hh Mm Ss' format."""
    sec = int(seconds)
    h, rem = divmod(sec, 3600)
    m, s = divmod(rem, 60)
    return f"{h}h {m}m {s}s"


def pair_call_events(events: List[Tuple[str, datetime, str]]) -> List[Dict[str, Any]]:
    """
    Match up start/end call events into individual sessions.
    Returns a list of dicts with start, end, server, and duration_sec.
    """
    stack = []
    sessions = []

    # Sort events by timestamp
    for event_type, ts, context in sorted(events, key=lambda x: x[1]):
        if event_type in CALL_START_EVENTS:
            stack.append((ts, context))
        elif event_type in CALL_END_EVENTS and stack:
            start_ts, start_ctx = stack.pop()
            sessions.append({
                "start": start_ts,
                "end": ts,
                "server": start_ctx or "Unknown/DM",
                "duration_sec": (ts - start_ts).total_seconds()
            })
    return sessions


def analyze_file(filepath: Path) -> Dict[str, Any]:
    """
    Read the JSON export line-by-line and collect:
    - event counts
    - call-related events
    - sent messages details
    """
    counters = Counter()
    call_events = []
    messages = []

    print(f"🔄 Analyzing file: {filepath.name}")
    with filepath.open('r', encoding='utf-8') as f:
        for line in f:
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            et = record.get("event_type")
            if not et:
                continue
            counters[et] += 1

            ts = record.get("timestamp")
            if not ts:
                continue
            dt = datetime.fromisoformat(ts.strip('"').replace("Z", "+00:00"))

            # Collect call events
            if et in CALL_START_EVENTS | CALL_END_EVENTS:
                ctx = record.get("guild_id") or record.get("channel_id")
                call_events.append((et, dt, ctx))
            # Collect message sends
            elif et == MESSAGE_EVENT:
                messages.append({
                    "timestamp": dt,
                    "channel": record.get("channel"),
                    "length": record.get("length", 0),
                    "word_count": record.get("word_count", 0)
                })

    print("✅ Analysis complete.")
    return {
        "event_counts": counters,
        "call_events": call_events,
        "messages": sorted(messages, key=lambda x: x["timestamp"], reverse=True)
    }


def export_call_sessions(sessions: List[Dict[str, Any]], csv_file: Path, json_file: Path) -> None:
    """Export call sessions to both CSV and JSON."""
    # CSV
    with csv_file.open('w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["Server/Channel", "Start", "End", "Duration_Readble", "Duration_Sec"])
        for s in sessions:
            writer.writerow([
                s["server"],
                s["start"].isoformat(),
                s["end"].isoformat(),
                format_duration(s["duration_sec"]),
                int(s["duration_sec"])
            ])
    print(f"📄 Call sessions CSV written to '{csv_file.name}'")

    # JSON
    serializable = [
        {
            "server": s["server"],
            "start": s["start"].isoformat(),
            "end": s["end"].isoformat(),
            "duration_readable": format_duration(s["duration_sec"]),
            "duration_sec": s["duration_sec"]
        } for s in sessions
    ]
    with json_file.open('w', encoding='utf-8') as f:
        json.dump(serializable, f, ensure_ascii=False, indent=2)
    print(f"📄 Call sessions JSON written to '{json_file.name}'")


def export_messages_log(messages: List[Dict[str, Any]], txt_file: Path) -> None:
    """Export sent messages log to a plain text file."""
    with txt_file.open('w', encoding='utf-8') as f:
        f.write("--- Sent Messages Log ---\n\n")
        for m in messages:
            ts = m['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
            f.write(f"[{ts}] Channel {m['channel']} — Len: {m['length']}, Words: {m['word_count']}\n")
    print(f"📄 Messages log written to '{txt_file.name}'")


def export_event_counts(counts: Counter, txt_file: Path) -> None:
    """Save event type frequencies to a text file."""
    with txt_file.open('w', encoding='utf-8') as f:
        for event_type, count in counts.most_common():
            f.write(f"{event_type}: {count}\n")
    print(f"📄 Event counts written to '{txt_file.name}'")


def main():
    fp = Path(input("Enter the path to your Discord JSON export file: ").strip().strip('"'))
    if not fp.is_file():
        print(f"❌ File not found: {fp}")
        return

    data = analyze_file(fp)
    event_counts = data["event_counts"]

    print("\n" + "=" * 40)
    print("📊 DISCORD DATA OVERVIEW")
    print("=" * 40)

    # 1) Calls
    print("\n📞 Call Analysis")
    sessions = pair_call_events(data["call_events"])
    if sessions:
        sessions_sorted = sorted(sessions, key=lambda x: x["start"], reverse=True)
        total_sec = sum(s["duration_sec"] for s in sessions_sorted)
        print(f"  • Total calls: {len(sessions_sorted)}")
        print(f"  • Total duration: {format_duration(total_sec)}")

        # Top 5 servers/channels by call time
        totals = defaultdict(float)
        for s in sessions_sorted:
            totals[s["server"]] += s["duration_sec"]
        ranking = sorted(totals.items(), key=lambda x: x[1], reverse=True)[:5]
        print("  🏆 Top 5 servers/channels by call duration:")
        for srv, dur in ranking:
            print(f"    - {srv}: {format_duration(dur)}")

        export_call_sessions(sessions_sorted, Path("discord_calls.csv"), Path("discord_calls.json"))
    else:
        print("  • No call activity found.")

    # 2) Streams
    print("\n📺 Stream Analysis")
    stream_count = event_counts.get(STREAM_START_EVENT, 0)
    print(f"  • You started streams {stream_count} times.")

    # 3) Messages
    print("\n✉️ Message Analysis")
    msgs = data["messages"]
    print(f"  • Total messages sent: {len(msgs)}")
    if msgs:
        export_messages_log(msgs, Path("sent_messages.txt"))

    # 4) All Event Stats
    print("\n📈 General Event Statistics")
    print("  • Top 10 most frequent events:")
    for et, cnt in event_counts.most_common(10):
        print(f"    - {et}: {cnt}")
    export_event_counts(event_counts, Path("event_type_counts.txt"))

    print("\n" + "=" * 40)
    print("✅ All analyses done and files saved!")
    print("=" * 40)


if __name__ == "__main__":
    main()
