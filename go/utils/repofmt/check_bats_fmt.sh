#!/bin/bash

script_dir=$(dirname "$0")
cd $script_dir/../../../integration-tests/bats

ERRORS_FOUND=0
for FILENAME_WITH_EXT in *.bats; do
    FILENAME=${FILENAME_WITH_EXT%".bats"}
    while read -r LINE; do
        if [[ ! "$LINE" =~ "@test \"$FILENAME:" ]]; then
            TESTNAME=$(echo "$LINE" | cut -d'"' -f 2)
            echo -e "ERROR: test \"$TESTNAME\" in \"$FILENAME_WITH_EXT\" must start with \"$FILENAME:\" in the title"
            ERRORS_FOUND=1
        fi
    done <<< $(grep '@test "' "$FILENAME_WITH_EXT")
done
if [[ $ERRORS_FOUND -eq 1 ]]; then
    exit 1
fi
exit 0