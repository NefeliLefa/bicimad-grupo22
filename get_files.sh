#!/bin/sh

FILES="
/public_data/bicimad/201704_movements.json
/public_data/bicimad/201705_movements.json
/public_data/bicimad/201706_movements.json
/public_data/bicimad/201707_movements.json
/public_data/bicimad/201708_movements.json
/public_data/bicimad/201709_movements.json
/public_data/bicimad/201710_movements.json
/public_data/bicimad/201711_movements.json
/public_data/bicimad/201712_movements.json
/public_data/bicimad/201801_movements.json
/public_data/bicimad/201802_movements.json
/public_data/bicimad/201803_movements.json
/public_data/bicimad/201804_movements.json
/public_data/bicimad/201805_movements.json
/public_data/bicimad/201806_movements.json
"

echo ":$FILES:"
for f in $FILES; do
    echo  `basename $f`
    ssh luis@picluster02.mat.ucm.es /opt/hadoop/current/bin/hadoop fs -get $f > `basename $f`
done
