#!/bin/bash

USER=root
PASS=
HOST=127.0.0.1
DBNAME=

function e {

for cfile in $1
do
	echo "Importing $cfile"
	cat $cfile | sed -e "s/DEFINER=\`he\`@\`localhost\`/DEFINER=\`root\`@\`localhost\`/" | mysql -h$HOST -u$USER -p$PASS -c $DBNAME 
done
}


function epre {

for cfile in $1
do
    echo "Importing $cfile"
    cat $cfile | sed -e "s/DEFINER=\`he\`@\`localhost\`/DEFINER=\`root\`@\`localhost\`/" | mysql -v -h$HOST -u$USER -c -p$PASS
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
