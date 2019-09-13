start_time=`date`
echo $start_time >> timer.txt
cat /home/hadoop/data/* > /home/hadoop/data/log2.txt
/usr/bin/hadoop dfs -put /home/hadoop/data/log2.txt /tmp/
rm -rf /home/hadoop/data/log2.txt

sh wrapper_regular.sh

messg=`cat status.txt | grep "Regular job is finshed"`

if [ "$messg" = "" ]; then
	while [ "$messg" = "" ]
	do
		sh wrapper_regular.sh
		messg=`cat status.txt | grep "Regular job is finshed"`	
	done
fi
start_time=`date`
echo $start_time >> timer.txt

/usr/bin/hadoop dfs -rm /tmp/log2.txt
