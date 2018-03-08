filename='filelist_test2'
filelines=`cat $filename`

for line in $filelines ; do
   echo $line
   hadoop fs -put -f $line input;
   spark-submit quote_spark.py input/$line;
   hadoop fs -rm -f input/$line;
done

