#!/bin/bash

# Lista serwerów do przywrócenia
INPUT_FILE="serwery_z_plikiem.txt"

# Folder docelowy w strukturze lokalnej
folder_restore="folder_restore"

# Katalogi robocze
DIR_BASE="/app/tools/bprestore"
DIR_LOGS="$DIR_BASE/logs"
DIR_RESTORE="$DIR_BASE/directory"
DIR_LISTS="$DIR_BASE/bplists"

# Utwórz katalogi jeśli nie istnieją
mkdir -p "$DIR_LOGS" "$DIR_RESTORE"

while read -r server; do
    [[ -z "$server" ]] && continue

    echo "Przywracanie danych dla serwera: $server"

    # Ścieżki plików
    filelist_server="$DIR_LISTS/bplist_${server}.txt"
    redirect_file="$DIR_RESTORE/directory_${server}"
    log_file="$DIR_LOGS/$server"

    # Tworzenie pliku zmiany ścieżki
    echo "change /app to /data/$folder_restore/$server" > "$redirect_file"

    # Wykonanie bprestore
    bprestore -A \
        -s "$startdate" -e "$enddate" \
        -C "$server" -S "$Server_netbackup" \
        -f "$filelist_server" \
        -L "$log_file" \
        -R "$redirect_file"

done < "$INPUT_FILE"

echo "✅ Zakończono przywracanie wszystkich serwerów."
