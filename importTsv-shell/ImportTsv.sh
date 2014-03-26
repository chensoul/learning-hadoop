#!/bin/bash

PROGRAM=`basename $0`
usage()
{
    echo -e "\n$PROGRAM: Import a table to HBase from CSV file."
    echo
    echo "Usage: $PROGRAM [-u user] [-b] [-c] [-z charset] -t <TableInfo FilePath> <TSV FilePath>"
    echo
    echo "  -u [user name], optional."
    echo "     Specify the user to do as for the import task. Default is hbase."
    echo "  -t <File path>, mandatory"
    echo "     The Table information file name."
    echo "  -b Perform a Bulk mode import."
    echo "  -c The input file is LZO-compressed file."
    echo "  -z The charset of rowkey-bytes. Default is UTF-8."
    echo
}
if [ $# -lt 1 ]
then
    usage
	exit 1
fi
SUDO_USER=hbase
while getopts hHu:t:z:bc opt
do
  case $opt in
    h | H) usage
           exit 0;;
    u)     SUDO_USER=$OPTARG;;
    t)     TABLE_INFO=$OPTARG;;
    b)     BULK_MODE="1";;
    c)     INPUT_LZO="1";;
    z)     CHARSET=$OPTARG;;
    \?)    usage
           exit 2;;
  esac
done
shift `expr $OPTIND - 1`

if [ -z $TABLE_INFO ]
then
  echo -e "\nStop: Please specify table information file path with -t argument."
  exit 1
fi

if [ ! -f $TABLE_INFO ]
then
  echo -e "\nStop: Can not reade file $TABLE_INFO."
  exit 1
fi

. $TABLE_INFO

if [ -z "$TABLE_NAME" ]
then
  echo -e "\nStop: No HBase table name defined as TABLE_NAME variable in $TABLE_INFO"
  exit 1
fi

if [ -z "$COLUMNS" ]
then
  echo -e "\nStop: No columns defined as COLUMNS variable in $TABLE_INFO"
  exit 1
fi

if [ -z "$KEY_COLUMNS" ]
then
  echo -e "\nStop: No KEY_COLUMNS variable defined in $TABLE_INFO"
  exit 1
fi

if [ -z "$COLUMNS_SEPARATOR" ]
then
  echo -e "\nStop: No COLUMNS_SEPARATOR variable defined in $TABLE_INFO"
  exit 1
fi


CSV_FILE=$1

if [ -n "$BULK_MODE" ]
then
  if [ -z "$BULKLOAD_HFILE_PATH" ]
  then
  	BULKLOAD_HFILE_PATH="/hbase"
  fi
  BULK_MODE="-Dimporttsv.bulk.output=$BULKLOAD_HFILE_PATH/${TABLE_NAME}_hfiles_tmp"
fi

if [ -n "$INPUT_LZO" ]
then
  INPUT_LZO="-Dimporttsv.input.codec=lzo"
fi
echo "hadoop fs -rm -r $BULKLOAD_HFILE_PATH/${TABLE_NAME}_hfiles_tmp"
sudo -u hdfs hadoop fs -rmr -skipTrash $BULKLOAD_HFILE_PATH/${TABLE_NAME}_hfiles_tmp
sudo -u $SUDO_USER hbase com.asp.tranlog.ImportTsv -Dimporttsv.separator=$COLUMNS_SEPARATOR -Dimporttsv.columns=$COLUMNS -Dimporttsv.key.columns=$KEY_COLUMNS -Dimporttsv.skip.bad.lines=true $BULK_MODE $INPUT_LZO $TABLE_NAME $CSV_FILE


if [ $? -eq 0 ]
then
    echo -e "\nCompleted."
    if [ -n "$BULK_MODE" ]
    then
        echo -e "\nNext, run following command to load the new imported data:\n"
        sudo -u $SUDO_USER hadoop jar /usr/lib/hbase/hbase.jar completebulkload  -Dhbase.bulkload.retries.number=5 $BULKLOAD_HFILE_PATH/${TABLE_NAME}_hfiles_tmp $TABLE_NAME
        echo -e "\nSuccessful."
    fi
else
    echo "Stopped with error."
fi
