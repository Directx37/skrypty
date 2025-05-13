#!/bin/bash

# Lista serwerów
SERVERS=("srv1" "srv2" "srv3")

# Fragment nazwy pliku do wyszukania
FRAGMENT="dane2024"

# Plik tymczasowy do zapisu wyników bplist
TMP_OUT="/tmp/bplist_tmp.txt"

# Plik wynikowy z serwerami, gdzie znaleziono pliki
OUTPUT_FILE="serwery_z_plikiem.txt"
> "$OUTPUT_FILE"  # wyczyść plik na start

# Tablice wyników
FOUND=()
NOT_FOUND=()

for server in "${SERVERS[@]}"; do
    echo -n "Sprawdzam [$server]... "

    > "$TMP_OUT"  # czyść plik tymczasowy

    # Twoja komenda bplist (zmienne niezmieniane)
    bplist -A -C "$server" -S "$Server_netbackup" -k "$file_CLT" -b -s "$startdate" -e "$enddate" -R /app/logs >> "$TMP_OUT" 2>/dev/null

    if grep -q "$FRAGMENT" "$TMP_OUT"; then
        echo "ZNALEZIONO"
        FOUND+=("$server")
        echo "$server" >> "$OUTPUT_FILE"
    else
        echo "brak"
        NOT_FOUND+=("$server")
    fi
done

echo ""
echo "=== Serwery z plikiem zawierającym fragment '$FRAGMENT' ==="
printf "%s\n" "${FOUND[@]}"

echo ""
echo "=== Serwery bez takiego pliku ==="
printf "%s\n" "${NOT_FOUND[@]}"

echo ""
echo "Lista serwerów z plikiem zapisana do: $OUTPUT_FILE"
