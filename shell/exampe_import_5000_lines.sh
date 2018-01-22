filename='trading_files.txt'
filelines=`cat $filename`
directory=taq.12.2014/

for line in $filelines ; do
   echo $line
   locate_dir=$'s3://'$directory$line
   sudo aws s3 cp $locate_dir ./temp.zip;
   name_file="$(unzip -l temp.zip | awk '/-----/ {p = ++p % 2; next} p {print $NF}')";
   unzip temp.zip;
   rm -f temp.zip;
   head -n 5000 $name_file  > tmp_file;
   mv tmp_file $name_file
done

