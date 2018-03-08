filename='filelist'
filelines=`cat $filename`
directory=taq.12.2014/

for line in $filelines ; do
   echo $line
   locate_dir=$'s3://'$directory$line
   sudo aws s3 cp $locate_dir ./temp.zip;
   name_file="$(unzip -l temp.zip | awk '/-----/ {p = ++p % 2; next} p {print $NF}')";
   unzip -o temp.zip;
   rm -f temp.zip;
   hadoop fs -put -f $name_file input;
   spark-submit quote_spark.py input/$name_file;
   hadoop fs -rm -f input/$name_file; 
   rm -f $name_file
done

