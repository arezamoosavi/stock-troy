#!/bin/sh


set -o errexit
set -o nounset


echo "wait"

sleep 40

python bokeh_app.py