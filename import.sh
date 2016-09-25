#!/bin/bash

USER=root
PASS=
HOST=127.0.0.1
DBNAME=

function e {

for cfile in $1
do
	echo "Importing $cfile"
	cat $cfile | mysql -h$HOST -u$USER -p$PASS $DBNAME 
done
}


function epre {

for cfile in $1
do
    echo "Importing $cfile"
    cat $cfile | mysql -h$HOST -u$USER -p$PASS
done
}

epre "pre.sql"
e "struc_*sql"
e "func_*sql"
e "proc_*sql"
e "event_*sql"
e "view_*sql"
echo "######### Workaround for view from view ugly structures ;)"
e "view_*sql"
e "data_*sql"
e "triggers_*sql"
