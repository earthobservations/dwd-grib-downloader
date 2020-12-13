#!/usr/bin/env bash

path=/var/www/static.mah.priv.at/cors/gribs
path=/Users/mah/Ballon/src/downloader/testme
model=icon-d2
extra=
extra='-v -d'
extra='-v'

# grid=regular-lat-lon
# single_level='relhum_2m u_10m v_10m vmax_10m t_2m'
# model_level='u v w t p qv'
# time_invariant='hhl'
# # multi-levels: 25-65
# # wieviele steps? 1 step = 1h  -> 4-20h
# min_level=1
# max_level=65
# max_step=27

grid=regular-lat-lon
single_level='vmax_10m'
model_level='u v'
time_invariant='hhl'
min_level=57
max_level=62
max_step=5
min_step=0
extra='-v -d'
extra=
path=test-downloader
#modelrun='--modelrun 2020121212'

latest_timestamp=`python3 opendata-downloader.py --get-latest-timestamp --model ${model} ${modelrun}`

gribdir=${path}/${latest_timestamp}

echo "gribdir:" ${gribdir}
echo "start:" `date`

mkdir -p ${gribdir}

if [ -n "$single_level" ]; then
  python3 opendata-downloader.py  --compressed  ${extra} \
        ${modelrun} \
        --model ${model}  \
        --grid  ${grid} \
        --single-level-fields ${single_level}  \
        --max-time-step ${max_step} \
        --min-time-step ${min_step} \
        --directory ${gribdir}
fi

if [ -n "$model_level" ]; then
  python3 opendata-downloader.py --compressed ${extra} \
        ${modelrun} \
        --model ${model}  \
        --grid  ${grid} \
        --model-level-fields  ${model_level}  \
        --min-time-step ${min_step}  \
        --max-time-step ${max_step}  \
        --min-model-level ${min_level}  \
        --max-model-level ${max_level}  \
        --directory ${gribdir}
fi

if [ -n "$time_invariant" ]; then
  python3 opendata-downloader.py --compressed ${extra} \
        ${modelrun} \
        --model ${model}  \
        --grid  ${grid} \
        --time-invariant-fields  ${time_invariant}  \
        --max-time-step 0  \
        --min-model-level ${min_level}  \
        --max-model-level ${max_level}  \
        --directory ${gribdir}
fi


echo "finish:" `date`
