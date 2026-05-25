#!/usr/bin/env bash

shopt -s failglob

infile="$(realpath "$1")"; shift
outdir=${1:-.}; shift

mkdir -p "$outdir"
cd "$outdir" || exit

# shellcheck disable=SC2016
awkscript='\
    /^###[[:space:]]+/ {
        sub(/^###[[:space:]]+/, "");
        sub(/[[:space:]]+$/, "");
        out=$0 ".tmp.txt";
        next
     }
     out {
        print > out
     }'

awk "$awkscript" "$infile"

for f in *.tmp.txt; do
    cat "$f" | aha > "$(basename "$f" .tmp.txt)".html
done

rm ./*.tmp.txt

tree -H '' | sed '/index\.html/d' > index.html
