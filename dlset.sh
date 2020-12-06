#!/usr/bin/env bash

path=/var/www/stiwoll.mah.priv.at/sandbox/gribs
model=icon-d2
extra='-v -d'
extra='-v'
extra=
grid=regular-lat-lon
single_level='relhum_2m v_10m vmax_10m t_2m'
model_level='u v w t p qv hhl'

# multi-levels: 25-65
# wieviele steps? 1 step = 1h  -> 4-20h
min_level=1
max_level=65
max_step=27


latest_timestamp=`python3 opendata-downloader.py --get-latest-timestamp --model ${model}`

gribdir=${path}/${model}/${latest_timestamp}

echo "gribdir:" ${gribdir}
echo "start:" `date`

mkdir -p ${gribdir}


python3 opendata-downloader.py  --compressed  ${extra} \
      --model ${model}  \
      --grid  ${grid} \
      --single-level-fields ${single_level}  \
      --max-time-step ${max_step}  --directory ${gribdir}


python3 opendata-downloader.py --compressed ${extra} \
      --model ${model}  \
      --grid  ${grid} \
      --model-level-fields  ${model_level}  \
      --max-time-step ${max_step}  \
      --min-model-level ${min_level}  \
      --max-model-level ${max_level}  \
      --directory ${gribdir}

echo "finish:" `date`
