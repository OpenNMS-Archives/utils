#!/bin/sh

FILE="$1"; shift
DB="$1"; shift
USER="opennms"

if [ -z "$DB" ]; then
	echo "usage: $0 <sql_filename> <database>"
	exit 1
fi

CAT_COMMAND="cat $FILE"

if [ `echo "$FILE" | grep -c -E '.gz$'` -gt 0 ]; then
	CAT_COMMAND="$CAT_COMMAND | gzip -dc"
fi

dropdb -U "$USER" "$DB"
createdb -U "$USER" -E utf8 "$DB"
$CAT_COMMAND | grep -v -E '^\\connect ' | psql -U "$USER" "$DB"
