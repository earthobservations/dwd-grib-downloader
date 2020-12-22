#!/usr/bin/env bash

# --- begin configuration
model=icon-d2

#extra='-v -d -f'  # verbose, dry run, flat structure
extra='--max-workers 10'

path=/tmp/test-downloader
grid=regular-lat-lon
single_level='vmax_10m'
model_level='u v'
pressure_level='u v'
pressure_levels='975 950'
time_invariant='hhl'
min_level=60
max_level=62
max_step=2
min_step=0

# --- end configuration

latest_timestamp=`python3 opendata-downloader.py --get-latest-timestamp --model ${model} ${modelrun}`

gribdir=${path}/${latest_timestamp}

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


if [ -n "$pressure_level" ]; then
  python3 opendata-downloader.py --compressed ${extra} \
        ${modelrun} \
        --model ${model}  \
        --grid  ${grid} \
        --pressure-level-fields  ${pressure_level}  \
        --min-time-step ${min_step}  \
        --max-time-step ${max_step}  \
        --pressure-levels ${pressure_levels} \
        --directory ${gribdir}
fi

if [ -n "$time_invariant" ]; then
  python3 opendata-downloader.py --compressed ${extra} \
        ${modelrun} \
        --model ${model}  \
        --grid  ${grid} \
        --time-invariant-fields  ${time_invariant}  \
        --directory ${gribdir}
        # --max-time-step 0  \
        # --min-model-level ${min_level}  \
        # --max-model-level ${max_level}  \

fi
