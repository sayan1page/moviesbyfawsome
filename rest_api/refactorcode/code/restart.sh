PID=`ps -edf|grep "uwsgi"|grep -v "grep"|awk '{print $2}'`

for p in $PID
do
	kill -9 $p
done

source /home/ec2-user/rest_api/venv/bin/activate
cd /home/ec2-user/rest_api/refactorcode/code/
nohup uwsgi --http :9000  --wsgi-file /home/ec2-user/rest_api/refactorcode/code/predictor_service.py --callable api &
