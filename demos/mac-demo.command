#!/bin/bash
# VIGIL demo launcher: broker, 3 subscribers, 3 publishers.
set -u
cd "$(dirname "$0")/.." || exit 1
DIR="$(pwd)"

command -v python3 >/dev/null || { echo "python3 not found"; exit 1; }

# broker strip on top, 3x2 client windows below
screen="$(osascript -e 'tell application "Finder" to get bounds of window of desktop' 2>/dev/null)" \
	|| screen="0,0,1512,982"
read -r L T R B <<<"$(echo "$screen" | tr -d ',')"
W=$((R - L))
TOP=$((T + 25))
BOT=$((TOP + (B - T) * 18 / 100))
MID=$(((BOT + B) / 2))
C1=$((L + W / 3))
C2=$((L + 2 * W / 3))

# hide this window so it doesn't cover a tile
MYTTY="$(tty 2>/dev/null)"
if [ "$MYTTY" != "not a tty" ]; then
	osascript >/dev/null 2>&1 <<EOF
tell application "Terminal"
	repeat with w in windows
		if tty of selected tab of w is "$MYTTY" then set miniaturized of w to true
	end repeat
end tell
EOF
fi

# cmd, title, left, top, right, bottom
launch() {
	osascript - "$1" "$2" "$3" "$4" "$5" "$6" <<'EOF'
on run argv
	set cmdText to item 1 of argv
	set winTitle to item 2 of argv
	set l to (item 3 of argv) as integer
	set tp to (item 4 of argv) as integer
	set r to (item 5 of argv) as integer
	set bt to (item 6 of argv) as integer
	tell application "Terminal"
		set beforeIDs to id of windows
		set t to do script cmdText
		-- do script does not always make its window front, so find it by id
		repeat with w in windows
			if (id of w) is not in beforeIDs then
				set custom title of t to winTitle
				set bounds of w to {l, tp, r, bt}
				exit repeat
			end if
		end repeat
	end tell
end run
EOF
}

launch "cd '$DIR' && python3 src/broker.py" "broker" \
	"$L" "$TOP" "$R" "$BOT"
sleep 2

launch "cd '$DIR' && python3 src/subscriber.py --as ResponseTeam" "ResponseTeam" \
	"$L" "$BOT" "$C1" "$MID"
launch "cd '$DIR' && python3 src/subscriber.py --as PublicShield" "PublicShield" \
	"$C1" "$BOT" "$C2" "$MID"
launch "cd '$DIR' && python3 src/subscriber.py --as ResearchArchive" "ResearchArchive" \
	"$C2" "$BOT" "$R" "$MID"

launch "cd '$DIR' && python3 src/publisher.py --as SightingReports" "SightingReports" \
	"$L" "$MID" "$C1" "$B"
launch "cd '$DIR' && python3 src/publisher.py --as PoltergeistActivity" "PoltergeistActivity" \
	"$C1" "$MID" "$C2" "$B"
launch "cd '$DIR' && python3 src/publisher.py --as ParanormalReadings" "ParanormalReadings" \
	"$C2" "$MID" "$R" "$B"

osascript -e 'tell application "Terminal" to activate' >/dev/null
