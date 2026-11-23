#!/bin/bash
# VIGIL seven-window demo wall. Each window owns its own real child process.
set -u
cd "$(dirname "$0")/.." || exit 1
DIR="$(pwd)"

command -v python3 >/dev/null || { echo "python3 not found"; exit 1; }

if pgrep -f "src/broker.py" >/dev/null; then
	echo "A broker is already running. Stop it before launching the wall." >&2
	exit 1
fi

if [ -d src/common/data ]; then
	BACKUP="${TMPDIR:-/tmp}/vigil-data-$(date +%Y%m%d-%H%M%S)"
	cp -R src/common/data "$BACKUP" || exit 1
	echo "Data backed up to $BACKUP"
fi

screen="$(osascript -e 'tell application "Finder" to get bounds of window of desktop' 2>/dev/null)" \
	|| screen="0,0,1512,982"
read -r L T R B <<<"$(echo "$screen" | tr -d ',')"
W=$((R - L))
TOP=$((T + 25))
BOT=$((TOP + (B - T) * 26 / 100))
MID=$(((BOT + B) / 2))
C1=$((L + W / 3))
C2=$((L + 2 * W / 3))

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

launch() {
	osascript - "$1" "$2" "$3" "$4" "$5" <<'EOF'
on run argv
	set cmdText to item 1 of argv
	set l to (item 2 of argv) as integer
	set tp to (item 3 of argv) as integer
	set r to (item 4 of argv) as integer
	set bt to (item 5 of argv) as integer
	tell application "Terminal"
		set beforeIDs to id of windows
		do script cmdText
		repeat with w in windows
			if (id of w) is not in beforeIDs then
				set bounds of w to {l, tp, r, bt}
				exit repeat
			end if
		end repeat
	end tell
end run
EOF
}

launch "cd '$DIR' && python3 -u src/tools/tui.py --role broker" \
	"$L" "$TOP" "$R" "$BOT"
sleep 2

for role in ResponseTeam PublicShield ResearchArchive; do
	case "$role" in
		ResponseTeam) c1="$L"; c2="$C1" ;;
		PublicShield) c1="$C1"; c2="$C2" ;;
		ResearchArchive) c1="$C2"; c2="$R" ;;
	esac
	launch "cd '$DIR' && python3 -u src/tools/tui.py --role $role" \
		"$c1" "$BOT" "$c2" "$MID"
done

for role in SightingReports PoltergeistActivity ParanormalReadings; do
	case "$role" in
		SightingReports) c1="$L"; c2="$C1" ;;
		PoltergeistActivity) c1="$C1"; c2="$C2" ;;
		ParanormalReadings) c1="$C2"; c2="$R" ;;
	esac
	launch "cd '$DIR' && python3 -u src/tools/tui.py --role $role" \
		"$c1" "$MID" "$c2" "$B"
done

osascript -e 'tell application "Terminal" to activate' >/dev/null
echo "Wall launched. Broker pane needs 120+ columns (231x13 target);"
echo "Terminal font size sets the column count - widen it if you see the resize message."
