#!/bin/bash
# Generate Mermaid xychart-beta markdown from CSV data.
#
# Usage:
#   bench/gen_chart.sh bar --title "Title" --y-label "Label" \
#       --labels "PsiTri,RocksDB,MDBX" --values "376691,126341,57138"
#
#   bench/gen_chart.sh line --title "Title" \
#       --x-col 2 --y-col 3 --x-label "Keys (M)" --y-label "Ops/sec (K)" \
#       --x-div 1000000 --y-div 1000 --sample 10 \
#       --legend "PsiTri" --color "#7b1fa2" file1.csv \
#       --legend "RocksDB" --color "#e65100" file2.csv
#
# Colors: purple=#7b1fa2 (PsiTri), orange=#e65100 (RocksDB),
#         blue=#1565c0 (MDBX), green=#16A34A, red=#DC2626
set -euo pipefail

MODE="${1:?Usage: gen_chart.sh <bar|line> [OPTIONS]}"
shift

case "$MODE" in
bar)
    TITLE="" Y_LABEL="" LABELS="" VALUES="" Y_MAX=""
    while [ $# -gt 0 ]; do
        case "$1" in
            --title)    TITLE="$2"; shift 2 ;;
            --y-label)  Y_LABEL="$2"; shift 2 ;;
            --labels)   LABELS="$2"; shift 2 ;;
            --values)   VALUES="$2"; shift 2 ;;
            --y-max)    Y_MAX="$2"; shift 2 ;;
            *) echo "Unknown option: $1" >&2; exit 1 ;;
        esac
    done

    # Format labels as ["A", "B", "C"]
    FMT_LABELS=$(echo "$LABELS" | awk -F, '{
        for(i=1;i<=NF;i++) { printf "%s\"%s\"", (i>1?", ":""), $i }
    }')

    # Format values as [1, 2, 3]
    FMT_VALUES=$(echo "$VALUES" | tr ',' ', ')

    # Auto y-max if not specified
    if [ -z "$Y_MAX" ]; then
        Y_MAX=$(echo "$VALUES" | tr ',' '\n' | sort -rn | head -1)
        # Round up to nice number
        Y_MAX=$(awk "BEGIN{v=$Y_MAX * 1.15; printf \"%d\", v}")
    fi

    cat <<MERMAID
\`\`\`mermaid
%%{init: {'theme': 'base', 'themeVariables': {'xyChart': {'plotColorPalette': '#2563EB'}}}}%%
xychart-beta
    title "$TITLE"
    x-axis [$FMT_LABELS]
    y-axis "$Y_LABEL" 0 --> $Y_MAX
    bar [$FMT_VALUES]
\`\`\`
MERMAID
    ;;

line)
    TITLE="" X_LABEL="" Y_LABEL="" X_COL=2 Y_COL=3 X_DIV=1 Y_DIV=1 SAMPLE=1 Y_MAX=""
    COLORS="" LEGENDS=""
    FILES=()

    while [ $# -gt 0 ]; do
        case "$1" in
            --title)    TITLE="$2"; shift 2 ;;
            --x-label)  X_LABEL="$2"; shift 2 ;;
            --y-label)  Y_LABEL="$2"; shift 2 ;;
            --x-col)    X_COL="$2"; shift 2 ;;
            --y-col)    Y_COL="$2"; shift 2 ;;
            --x-div)    X_DIV="$2"; shift 2 ;;
            --y-div)    Y_DIV="$2"; shift 2 ;;
            --sample)   SAMPLE="$2"; shift 2 ;;
            --y-max)    Y_MAX="$2"; shift 2 ;;
            --legend)   LEGENDS="${LEGENDS:+$LEGENDS,}$2"; shift 2 ;;
            --color)    COLORS="${COLORS:+$COLORS,}$2"; shift 2 ;;
            *)
                if [ -f "$1" ]; then
                    FILES+=("$1")
                else
                    echo "Unknown option or missing file: $1" >&2; exit 1
                fi
                shift ;;
        esac
    done

    if [ ${#FILES[@]} -eq 0 ]; then
        echo "No CSV files specified" >&2; exit 1
    fi

    # Build x-axis from first file (assume all share same x range)
    X_AXIS=$(awk -F, -v col="$X_COL" -v div="$X_DIV" -v samp="$SAMPLE" '
        NR==1 { next }  # skip header
        (NR-2) % samp == 0 { printf "%s%d", (n++>0?", ":""), $col/div }
    ' "${FILES[0]}")

    # Build color palette
    if [ -n "$COLORS" ]; then
        PALETTE="$COLORS"
    else
        PALETTE="#7b1fa2,#e65100,#1565c0,#16A34A,#DC2626"
    fi

    # Auto y-max
    if [ -z "$Y_MAX" ]; then
        MAX_VAL=0
        for f in "${FILES[@]}"; do
            fmax=$(awk -F, -v col="$Y_COL" -v div="$Y_DIV" '
                NR>1 { v=$col/div; if(v>m) m=v } END { printf "%d", m }
            ' "$f")
            [ "$fmax" -gt "$MAX_VAL" ] && MAX_VAL=$fmax
        done
        Y_MAX=$(awk "BEGIN{printf \"%d\", $MAX_VAL * 1.15}")
    fi

    # Print chart
    echo '```mermaid'
    echo '---'
    echo 'config:'
    echo '    theme: base'
    echo '    themeVariables:'
    echo '        xyChart:'
    echo '            backgroundColor: "#ffffff"'
    echo "            plotColorPalette: \"$PALETTE\""
    echo '---'
    echo 'xychart-beta'
    echo "    title \"$TITLE\""
    echo "    x-axis \"$X_LABEL\" [$X_AXIS]"
    echo "    y-axis \"$Y_LABEL\" 0 --> $Y_MAX"

    for f in "${FILES[@]}"; do
        DATA=$(awk -F, -v col="$Y_COL" -v div="$Y_DIV" -v samp="$SAMPLE" '
            NR==1 { next }
            (NR-2) % samp == 0 { printf "%s%d", (n++>0?", ":""), $col/div }
        ' "$f")
        echo "    line [$DATA]"
    done

    echo '```'

    # Print legend note if legends specified
    if [ -n "$LEGENDS" ]; then
        echo ""
        echo '!!! note "Chart legend"'
        IFS=',' read -ra LEG_ARR <<< "$LEGENDS"
        IFS=',' read -ra COL_ARR <<< "$PALETTE"
        for i in "${!LEG_ARR[@]}"; do
            color="${COL_ARR[$i]:-#000}"
            echo "    ${LEG_ARR[$i]}"
        done
    fi
    ;;

*)
    echo "Usage: gen_chart.sh <bar|line> [OPTIONS]" >&2
    exit 1
    ;;
esac
