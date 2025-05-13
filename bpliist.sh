#!/bin/bash

# Plik wejściowy z listą serwerów
INPUT_FILE="serwery_z_plikiem.txt"

# Folder docelowy na wyniki
OUTPUT_DIR="/app/tools/bprestore"
mkdir -p "$OUTPUT_DIR"

# Parametry NetBackup — używasz swoich zmiennych
# Nie ruszam: $Server_netbackup, $file_CLT, $startdate, $enddate

while read -r server; do
    [[ -z "$server" ]] && continue  # pomiń puste linie

    echo "Tworzę bplist dla serwera: $server"

    bplist -A -C "$server" -S "$Server_netbackup" -k "$file_CLT" -b -s "$startdate" -e "$enddate" -R /app/logs \
        >> "$OUTPUT_DIR/bplist_${server}.txt" 2>/dev/null

done < "$INPUT_FILE"

echo "Gotowe. Pliki zapisane w: $OUTPUT_DIR"
