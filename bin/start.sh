export PYTHONPATH=`pwd`
WWWROOT=/data/wwwroot
mkdir logs
mkdir -p $WWWROOT
python utils/ks_manager.py
python maker/ks_maker.py $WWWROOT
chown -R www:www $WWWROOT