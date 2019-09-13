start_time=`date`
echo $start_time >> timer.txt
sh wrapper.sh
messg=`cat status.txt | grep "Starting job is finshed"`

if [ "$messg" = "" ]; then
	while [ "$messg" = "" ]
	do
		sh wrapper_regular.sh
		messg=`cat status.txt | grep "Regular job is finshed"`	
	done
fi
start_time=`date`
echo $start_time >> timer.txt
