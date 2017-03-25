#!/usr/bin/env bash

# Exit upon any errors
set -e

# Set defaults
HOST=
PORT=
USER=root
PASSWORD=
DATABASE=controller
DRYRUN=false

usage() {
  echo "Usage: create_schemas.h [-H|--host HOST] [-P|--port PORT]"
  echo "                        [-u|--user USERNAME] [-p|--password PASSWORD]"
  echo "                        [-d|--database DATABASE] [-v|--version VERSION]"
  echo "                        [--dry-run] [-h|--help]"
  echo "optional arguments:"
  echo "-h, --help              show this help message and exit"
  echo "-H, --host HOST         DB server host name, default is localhost"
  echo "-P, --port HOST         DB server port number, default is 3306"
  echo "-u, --user USERNAME     DB server user name, default is root"
  echo "-p, --password PASSWORD DB server password, default is empty"
  echo "-d, --database DATABASE database to use, default is controller"
  echo "--dry-run               print create_schemas route without action"
}


while [[ $# -ge 1 ]]
do
key="$1"

case $key in
    -H|--host)
    HOST="$2"
    shift # past argument
    ;;
    -P|--port)
    PORT="$2"
    shift # past argument
    ;;
    -u|--user)
    USER="$2"
    shift # past argument
    ;;
    -p|--password)
    PASSWORD="$2"
    shift # past argument
    ;;
    -d|--database)
    DATABASE="$2"
    shift # past argument
    ;;
    -h|--help)
    usage #unknown option
    exit 0
    ;;
    --dry-run)
    DRYRUN=true
    ;;
    *)
    echo "Unknown option $1"
    usage #unknown option
    exit 1
    ;;
esac
shift # past argument or value
done

[ ! -z "$HOST" ] && HOST="--host=$HOST"
[ ! -z "$PORT" ] && PORT="--port=$PORT"
[ ! -z "$PASSWORD" ] && PASSWORD="--password=$PASSWORD"

# get the directory of this script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"


if [ "$DRYRUN" == "true" ]; then
  echo "Dry run, exiting..."
  exit
fi

mysql $HOST $PORT -u $USER $PASSWORD $DATABASE < $DIR/create_tables.sql

echo "Successfully created tables"