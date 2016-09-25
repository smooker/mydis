#!/usr/bin/perl -w
use DBD::mysql;
use Getopt::Long;
use strict;
use warnings;

GetOptions(
    "skip=s" => \my @skiptables,
    "host=s" => \my $dbhost,
    "name=s" => \my $dbname,
    "user=s" => \my $dbuser,
    "pass=s" => \my $dbpass,
);

my %skipTables = map { $_ => 1 } @skiptables;

my $dbh = DBI->connect("DBI:mysql:database=$dbname;host=$dbhost;mysql_connect_timeout=1",
                         "$dbuser", "$dbpass",
                         {'RaiseError' => 1});

my %users = ();

my %privTables = (
    'user' => 1,
    'db' => 1,
    'tables_priv' => 1,
    'columns_priv' => 1,
    'procs_priv' => 1,
    'proxies_priv' => 1,
);

my %tables = ();

sub getTables()
{
    my %tables;
    
    my $sth = $dbh->prepare("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA='$dbname' AND TABLE_TYPE='BASE TABLE'");
    $sth->execute();
    
    while (my $ref = $sth->fetchrow_hashref()) {
        $tables{$ref->{'TABLE_NAME'}} = 1000;
    }
    return %tables;
}

sub getFieldNames {
        my $tableName = shift;
        my %retHash = ();
        my $fieldNum = 0;

#	print "\nGetting column names for table \"$tableName\" \n";
        my $sth = $dbh->prepare("SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='mysql' AND TABLE_NAME='$tableName'");
        $sth->execute();
        while (my $ref = $sth->fetchrow_hashref()) {
#		printf("\tCOLUMN_NAME: $ref->{'COLUMN_NAME'}\n");
                $retHash{$fieldNum++} = $ref->{'COLUMN_NAME'};
        }
        return %retHash;
}

sub getUsers()
{
        my $userNum = 0;
        my %retHash = ();
        my $sth = $dbh->prepare("SELECT User, Host FROM mysql.user ORDER BY User, Host");
        $sth->execute();

    while (my $ref = $sth->fetchrow_hashref()) {
                $retHash{$userNum++} = $ref->{'User'};
                #.'@'."'".$ref->{'Host'}."'";
        }
        return %retHash;
}

sub buildDeleteQuery
{
        my $table = shift;
        my $user = shift;
        my $query = '';

        $query .= "############################################\nDELETE FROM `mysql`.$table WHERE User='$user';\n";
        return $query;
}

sub buildInsertQuery
{
    my $table = shift;
    my $user = shift;
    my $query = '';

    my $sth = $dbh->prepare("SELECT * FROM `mysql`.$table WHERE User='$user'");
    $sth->execute();
    while (my $ref = $sth->fetchrow_hashref()) {
        $query .= "REPLACE INTO `mysql`.$table SET\n";
        my %fieldNames = getFieldNames($table);
        foreach my $field (sort{$a <=> $b}(keys %fieldNames)) {
                my $value = qq('');
                $value = "'".$ref->{$fieldNames{$field}}."'" unless !$ref->{$fieldNames{$field}};
                if ( $fieldNames{$field} eq 'Timestamp' ) {
                        $value = "NOW()";
                }
            if ( $fieldNames{$field} eq 'max_questions' ) {
                $value = "0";
            }
                            if ( $fieldNames{$field} eq 'max_updates' ) {
                $value = "0";
            }
            if ( $fieldNames{$field} eq 'max_connections' ) {
                $value = "0";
            }
            if ( $fieldNames{$field} eq 'max_user_connections' ) {
                $value = "0";
            }
                            $query .= "\t$fieldNames{$field} = $value";
                            if ( ($field+1) != keys %fieldNames ) {
                                    $query .= ",\n";
                            } else {
                                    $query .= "\n;\n";
                            }
            }
    }
    return $query;
}

sub getGtidPos
{
    my $binlog_file = shift;
    my $binlog_pos = shift;
    my $gtid = "";
    my $sth = $dbh->prepare("SELECT BINLOG_GTID_POS(?, ?) AS POS");
    $sth->execute($binlog_file, $binlog_pos);

    while (my $ref = $sth->fetchrow_hashref()) {
            $gtid = $ref->{'POS'};
    }

    return $gtid;
}

sub getMasterInfo()
{
    my $binlog_file;
    my $binlog_pos;
    my $gtid;			#later

    my $sth = $dbh->prepare("SELECT VARIABLE_NAME,VARIABLE_VALUE FROM information_schema.GLOBAL_STATUS WHERE VARIABLE_NAME IN ('BINLOG_SNAPSHOT_FILE','BINLOG_SNAPSHOT_POSITION')");
    $sth->execute();

    while (my $ref = $sth->fetchrow_hashref()) {
        if ($ref->{'VARIABLE_NAME'} eq "BINLOG_SNAPSHOT_FILE") {
                $binlog_file = $ref->{'VARIABLE_VALUE'};
        }
        if ($ref->{'VARIABLE_NAME'} eq "BINLOG_SNAPSHOT_POSITION") {
            $binlog_pos = $ref->{'VARIABLE_VALUE'};
        }
    }
    return $binlog_file, $binlog_pos;
}

#na koi full dump struc+data
%tables = (
    config =>  '100',
    users =>  '100',
);

%tables = getTables();

#print "Skipping tables $skipTables{'images_files2'}\n";

print "Dumping database creation\n";
open(FILE,">pre.sql");
print FILE "DROP DATABASE IF EXISTS ".$dbname.";\n"; 
print FILE "CREATE DATABASE ".$dbname." DEFAULT CHARACTER SET=utf8 DEFAULT COLLATE=utf8_general_ci;\n"; 
close FILE;

print "Dumping master info\n";
open(FILE,">master_info.sql");
print FILE getMasterInfo();
print FILE getGtidPos(getMasterInfo());
close FILE;

%users = getUsers();

#%users = (
#        "asdf" => "%",
#);

print "Dumping users privileges\n";
open(FILE,">users_privs.sql");
foreach my $user1 (values %users) {
    foreach my $table (keys %privTables) {
        print FILE buildDeleteQuery($table, $user1);
        print FILE buildInsertQuery($table, $user1);
    }
}
close FILE;

    #export na TABLES
    my $sth = $dbh->prepare("SET NAMES UTF8");
    $sth->execute();
    $sth = $dbh->prepare("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA='$dbname' AND TABLE_TYPE='BASE TABLE'");
    $sth->execute();
    
    while (my $ref = $sth->fetchrow_hashref()) {
        printf("Export of TABLE $ref->{'TABLE_NAME'} structure!\n");
        my $sth1 = $dbh->prepare("SHOW CREATE TABLE $ref->{'TABLE_NAME'}");
        $sth1->execute();
        my $ref1 = $sth1->fetchrow_hashref();
        $ref1->{'Create Table'} =~ m/ORD:(\d{3})/;
        my $order="000";
        if ($1) {$order=$1;}
        system("rm -f struc_".$ref->{'TABLE_NAME'}.".sql");
        open(FILE8,">struc_".$ref->{'TABLE_NAME'}.".sql");
        print FILE8 "SET foreign_key_checks = 0;\n";
        print FILE8 "DROP TABLE IF EXISTS `$ref->{'TABLE_NAME'}`;\n";
        print FILE8 "SET foreign_key_checks = 0;\n";
        $ref1->{'Create Table'} =~ s/AUTO_INCREMENT=(\d+)/AUTO_INCREMENT=1/g;
        print(FILE8 $ref1->{'Create Table'}.";\n");
        close FILE8;

        if(exists($skipTables{$ref->{'TABLE_NAME'}}))
        {
            print "Skipping table $ref->{'TABLE_NAME'}\n";
            next;
        }

        if (defined $tables{$ref->{'TABLE_NAME'}})
        {
	#da exportvame data do reasonable limit
	printf("Dumping data for table $ref->{'TABLE_NAME'}\n");
	system("rm -f data_*".$ref->{'TABLE_NAME'}.".sql");
        system("mysqldump -h$dbhost --where='1 LIMIT $tables{$ref->{'TABLE_NAME'}}' -t --skip-dump-date --skip-quick --complete-insert --extended-insert --insert-ignore --triggers=FALSE -u$dbuser -p$dbpass $dbname $ref->{'TABLE_NAME'} > data_$ref->{'TABLE_NAME'}.sql");
        }
        
        system("rm -f triggers_".$ref->{'TABLE_NAME'}.".sql");

        #export na TRIGGERS
        printf("\tExport of TRIGGERS for $ref->{'TABLE_NAME'}!\n");
        my $sth2 = $dbh->prepare("SELECT TRIGGER_NAME, TRIGGER_SCHEMA, EVENT_MANIPULATION, ACTION_TIMING FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA='$dbname' AND information_schema.TRIGGERS.EVENT_OBJECT_TABLE='$ref->{'TABLE_NAME'}'");
        $sth2->execute();
        
        while (my $ref2 = $sth2->fetchrow_hashref()) {
	printf("\t\tExport of TRIGGER $ref2->{'TRIGGER_NAME'}!\n");
	my $sth3 = $dbh->prepare("SHOW CREATE TRIGGER $ref2->{'TRIGGER_NAME'}");
	$sth3->execute();
	my $ref3 = $sth3->fetchrow_hashref();
	open(FILE4,">>triggers_".$ref->{'TABLE_NAME'}.".sql");
	print(FILE4 "#############################################################################################\n");
	print(FILE4 "#############################################################################################\n");
	print(FILE4 "#############################################################################################\n");
	print FILE4 "DROP TRIGGER IF EXISTS $ref2->{'TRIGGER_NAME'};\n";
	print FILE4 "DELIMITER ;;\n";
	print(FILE4 $ref3->{'SQL Original Statement'}."\n");
	print FILE4 ";;\nDELIMITER ;\n";
	close FILE4;
        }
    }

    #export na VIEW
    $sth = $sth = $dbh->prepare("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA='$dbname' AND TABLE_TYPE='VIEW'");

        $sth->execute();
    
    while (my $ref = $sth->fetchrow_hashref()) {
        printf("Export of VIEW ".$ref->{'TABLE_NAME'}."\n");
        open(FILE,">view_".$ref->{'TABLE_NAME'}.".sql");
        print FILE "DROP VIEW IF EXISTS $ref->{'TABLE_NAME'};\n";
        my $sth1 = $dbh->prepare("SHOW CREATE VIEW $ref->{'TABLE_NAME'}");
        $sth1->execute();
        my $ref1 = $sth1->fetchrow_hashref();
        print FILE "DELIMITER ;;\n";

        print(FILE $ref1->{'Create View'}."\n");
        print "Processing view = $ref->{'TABLE_NAME'}\n";
        print FILE  ";;\nDELIMITER ;\n";
        close FILE;
    }
    
    #export na EVENTS
    
    $sth = $sth = $dbh->prepare("SELECT EVENT_NAME, EVENT_DEFINITION FROM information_schema.EVENTS WHERE EVENT_SCHEMA='$dbname'");
        $sth->execute();
    
    while (my $ref = $sth->fetchrow_hashref()) {
        printf("Export of EVENT ".$ref->{'EVENT_NAME'}."\n");
        open(FILE,">event_".$ref->{'EVENT_NAME'}.".sql");
        my $sth1 = $dbh->prepare("SHOW CREATE EVENT \`$ref->{'EVENT_NAME'}\`");
        $sth1->execute();
        my $ref1 = $sth1->fetchrow_hashref();
        print FILE "DROP EVENT IF EXISTS $ref->{'EVENT_NAME'};\n";
        print FILE "DELIMITER ;;\n";
        print(FILE $ref1->{'Create Event'}."\n");
        print "Processing event = $ref->{'EVENT_NAME'}\n";
        print FILE  ";;\nDELIMITER ;\n";
        close FILE;
    }

    #export na FUNCTIONS

    $sth = $sth = $dbh->prepare("SELECT SPECIFIC_NAME FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA='$dbname' AND ROUTINE_TYPE='FUNCTION'");
    $sth->execute();
    
    while (my $ref = $sth->fetchrow_hashref()) {
        printf("Export of FUNCTION ".$ref->{'SPECIFIC_NAME'}."\n");
        open(FILE,">func_".$ref->{'SPECIFIC_NAME'}.".sql");
        my $sth1 = $dbh->prepare("SHOW CREATE FUNCTION $ref->{'SPECIFIC_NAME'}");
        $sth1->execute();
        my $ref1 = $sth1->fetchrow_hashref();
        print FILE "DROP FUNCTION IF EXISTS $ref->{'SPECIFIC_NAME'};\n";
        print FILE "DELIMITER ;;\n";
        print(FILE $ref1->{'Create Function'}."\n");
        print "Processing function = $ref->{'SPECIFIC_NAME'}\n";
        print FILE  ";;\nDELIMITER ;\n";
        close FILE;
    }

    #export na PROCEDURES

    $sth = $sth = $dbh->prepare("SELECT SPECIFIC_NAME FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA='$dbname' AND ROUTINE_TYPE='PROCEDURE'");
    $sth->execute();
    
    while (my $ref = $sth->fetchrow_hashref()) {
        printf("Export of PROCEDURE ".$ref->{'SPECIFIC_NAME'}."\n");
        open(FILE,">proc_".$ref->{'SPECIFIC_NAME'}.".sql");
        my $sth1 = $dbh->prepare("SHOW CREATE PROCEDURE $ref->{'SPECIFIC_NAME'}");
        $sth1->execute();
        my $ref1 = $sth1->fetchrow_hashref();
        print FILE "DROP PROCEDURE IF EXISTS $ref->{'SPECIFIC_NAME'};\n";
        print FILE "DELIMITER ;;\n";
        print(FILE $ref1->{'Create Procedure'}."\n");
        print "Processing procedure = $ref->{'SPECIFIC_NAME'}\n";
        print FILE  ";;\nDELIMITER ;\n";
        close FILE;
    }

