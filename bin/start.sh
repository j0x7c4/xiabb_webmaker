export PYTHONPATH=`pwd`
WWWROOT=/data/wwwroot
mkdir logs
mkdir -p $WWWROOT
python utils/$1_manager.py
python maker/$1_maker.py $WWWROOT
chown -R www:www $WWWROOT