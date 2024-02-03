#!/bin/bash

usage() {
  cat << \
=================================================================================================================================
Daily Top Songs script to set up the Postgres database.

Usage:
  $0 [-hDslb] [-F filepath_sql] [-f filepath_csv] [-p port] [-d database] [-u user] <host>

Flags:
  -h help             Show this help menu
  -D debug            Enables debug mode
  -s structure        Cleans up schema and creates types and tables
  -l load             Loads seed data from CSV files into the tables
  -b behavior         Creates or replaces indexes, functions, triggers, and views

Options:
  -F filepath_sql     Filepath of SQL files to be loaded into the tables (default: '$(pwd)/sql')
  -f filepath_csv     Filepath of CSV files to be loaded into the tables (default: '$(pwd)/csv')
  -p port             Port number of Postgres server (default: 5432)
  -d database         Name of database in the Postgres server (default: 'postgres')
  -u user             Name of user in the Postgres server (default: 'postgres')

Arguments:
  <host>              The hostname or IP address of the Postgres server

Notes:
  If the -h flag is used, the script will output the appropriate information and exit, with the first flag taking priority.

  If none of the -s, -l, or -b flags are used, then the command will assume that all of them were used.

  If the -f option is used but the -l flag is not used, the -f option and the argument passed will have no effect.
=================================================================================================================================
}

d_flag=false

slb_flag_set=false
s_flag=false
l_flag=false
b_flag=false

filepath_sql="$(pwd)/sql"
filepath_csv="$(pwd)/csv"
port=5432
database="postgres"
user="postgres"

while getopts "hvDslbF:f:p:d:u:" opt; do
  case "$opt" in
    h)
      usage
      exit 0
      ;;
    D)
      d_flag=true
      ;;
    s)
      slb_flag_set=true
      s_flag=true
      ;;
    l)
      slb_flag_set=true
      l_flag=true
      ;;
    b)
      slb_flag_set=true
      b_flag=true
      ;;
    F)
      filepath_sql="$OPTARG"
      ;;
    f)
      filepath_csv="$OPTARG"
      ;;
    p)
      port="$OPTARG"
      ;;
    d)
      database="$OPTARG"
      ;;
    u)
      user="$OPTARG"
      ;;
    *)
      echo ""
      usage
      exit 1
  esac
done

shift $((OPTIND - 1))

if [ "$#" != 1 ]; then
  echo -e ""$0": incorrect number of arguments given -- "$#"\n"
  usage
  exit 1
fi

if ! "$slb_flag_set"; then
  s_flag=true
  l_flag=true
  b_flag=true

  if "$d_flag"; then
    echo "-s, -l, and -b flags were not used. Script will continue assuming all of them were used."
  fi
fi

host=$1

if "$d_flag"; then
  echo -e "Flags:\n  -D debug"

  if "$s_flag"; then
    echo "  -s structure"
  fi

  if "$l_flag"; then
    echo "  -l load"
  fi

  if "$b_flag"; then
    echo "  -b behavior"
  fi

  cat << \
=================================================================================================================================

Options:
  -F filepath_sql:    $filepath_sql
  -f filepath_csv:    $filepath_csv
  -p port:            $port
  -d database:        $database
  -u user:            $user

Arguments:
  <host>              $host

=================================================================================================================================
fi

psql_credentials="-h "$host" -p "$port" -d "$database" -U "$user""

if "$d_flag"; then
  echo "Creating function to run SQL files."
fi

run_sql_file() {
  if "$d_flag"; then
    local command="psql -a -b -e -E "$psql_credentials" -f \""$filepath_sql"/"$1"\""
    echo "Executing command: "$command""
  else
    local command="psql "$psql_credentials" -f \""$filepath_sql"/"$1"\""
  fi

  eval "$command"
  local exit_code="$?"

  if "$d_flag"; then
    echo "Command finished executing with exit code "$exit_code"."
  fi

  if [ "$exit_code" != 0 ]; then
    echo ""$0": psql command resulted in non-zero exit code -- "$exit_code""
    exit 1
  fi
}

if "$d_flag"; then
  echo "Creating function to run a single SQL or internal PSQL command"
fi

run_psql_command() {
  if "$d_flag"; then
    local command="psql -a -b -e -E "$psql_credentials" -c \""$1"\""
    echo "Executing command: "$command""
  else
    local command="psql "$psql_credentials" -c \""$1"\""
  fi

  eval "$command"
  local exit_code="$?"

  if "$d_flag"; then
    echo "Command finished executing with exit code "$exit_code"."
  fi

  if [ "$exit_code" != 0 ]; then
    echo ""$0": psql command resulted in non-zero exit code -- "$exit_code""
    exit 1
  fi
}

if "$d_flag"; then
  echo "Script is starting."
fi

if "$s_flag"; then
  if "$d_flag"; then
    echo "-s flag stage is starting."
    echo "Cleaning up schema and creating types and tables."
  fi

  run_sql_file "create_schema_structure.sql"

  if "$d_flag"; then
    echo "-s flag stage is ending."
  fi
fi

if "$l_flag"; then
  if "$d_flag"; then
    echo "-l flag stage is starting."
    echo "Creating temporary tables with no constraints to client-side copy the CSV data into."
  fi

  run_sql_file "create_temporary_tables.sql"

  filename_artist_csv="artist.csv"

  if "$d_flag"; then
    echo "Loading seed data from CSV files into the temporary tables."
    echo "Loading seed data from "$filename_artist_csv" into the respective temporary table."
  fi

  run_psql_command "\\\copy music_data.temp_artist_tb (artist_id, artist_name) FROM '"$filepath_csv"/"$filename_artist_csv"' WITH (FORMAT csv, HEADER, DELIMITER ',')"

  filename_song_csv="song.csv"

  if "$d_flag"; then
    echo "Loading seed data from "$filename_song_csv" into the respective temporary table."
  fi

  run_psql_command "\\\copy music_data.temp_song_tb (isrc, song_name, song_duration_ms, is_explicit, spotify_url, apple_music_url) FROM '"$filepath_csv"/"$filename_song_csv"' WITH (FORMAT csv, HEADER, DELIMITER ',')"

  filename_artist_song_map_csv="artist_song_map.csv"

  if "$d_flag"; then
    echo "Loading seed data from "$filename_artist_song_map_csv" into the respective temporary table."
  fi

  run_psql_command "\\\copy music_data.temp_artist_song_map_tb (artist_id, isrc) FROM '"$filepath_csv"/"$filename_artist_song_map_csv"' WITH (FORMAT csv, HEADER, DELIMITER ',')"

  filename_ranking_csv="ranking.csv"

  if "$d_flag"; then
    echo "Loading seed data from "$filename_ranking_csv" into the respective temporary table."
  fi
  
  run_psql_command "\\\copy music_data.temp_ranking_tb (isrc, ranking_date, rank, ranking_source) FROM '"$filepath_csv"/"$filename_ranking_csv"' WITH (FORMAT csv, HEADER, DELIMITER ',')"

  if "$d_flag"; then
    echo "Inserting seed data from temporary tables to real tables and cleaning up temporary tables."
  fi

  run_sql_file "load_seed_data.sql"

  if "$d_flag"; then
    echo "-l flag stage is ending."
  fi
fi

if "$b_flag"; then
  if "$d_flag"; then
    echo "-b flag stage is starting."
    echo "Creating or replacing indexes, functions, triggers, and views."
  fi

  run_sql_file "create_schema_behavior.sql"

  if "$d_flag"; then
    echo "-b flag stage is ending."
  fi
fi

if "$d_flag"; then
  echo "Script is ending."
fi

exit 0
