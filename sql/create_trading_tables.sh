for file in ~/db/trade_data/* ; do 
 echo $file;
 date=${file:(-8)};
 PGPASSFILE=.pgpass psql -h dbbigdata.czzdxt0nkcse.us-east-2.rds.amazonaws.com -U linxy0331 -d bigData -f drop_tables.sql -v d=$date
 PGPASSFILE=.pgpass psql -h dbbigdata.czzdxt0nkcse.us-east-2.rds.amazonaws.com -U linxy0331 -d bigData -f create_tables.sql -v d=$date
 schema='(A,B)'; 
 sed  '1 d' $file | sed 's/[^[:alnum:]]\+$//;s/ \{1,\}/,/g' | sed 's/,/-/2g' | sed '1i A,B' | sed $'s/\r$//' | sed '/^$/d' | PGPASSFILE=.pgpass psql -h dbbigdata.czzdxt0nkcse.us-east-2.rds.amazonaws.com -U linxy0331 -d bigData -c "COPY temporal_trading ${schema} FROM stdin CSV HEADER;"
 PGPASSFILE=.pgpass psql -h dbbigdata.czzdxt0nkcse.us-east-2.rds.amazonaws.com -U linxy0331 -d bigData -f populate_temporal_trading_table.sql -v d=$date
done
