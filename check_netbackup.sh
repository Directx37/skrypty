#!/bin/bash

# Lista serwerów (NetBackup clients)
SERVERS=("serwer1" "serwer2" "serwer3" "serwer4")

# Szukany fragment nazwy pliku (np. część wspólna typu "dane2024")
SEARCH_FRAGMENT="dane2024"

# Parametry NetBackup
POLICY_TYPE=0         # 0 = Standard filesystem backup
DEPTH=5               # Głębokość rekursji
START_PATH="/"        # Główna ścieżka do przeszukania

echo "Szukam plików zawierających '$SEARCH_FRAGMENT' w backupie NetBackup..."

for SERVER in "${SERVERS[@]}"; do
    echo -n "[$SERVER] ... "

    FILES=$(bplist -C "$SERVER" -t $POLICY_TYPE -R $DEPTH "$START_PATH" 2>/dev/null | grep "$SEARCH_FRAGMENT")

    if [[ -n "$FILES" ]]; then
        echo "ZNALEZIONO:"
        echo "$FILES" | sed 's/^/    /'
    else
        echo "brak"
    fi
done
