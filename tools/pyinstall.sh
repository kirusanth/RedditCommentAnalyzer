#!/bin/bash

if [[ -f ./requirements.txt ]]; then
  pip install --no-cache-dir -r ./requirements.txt
else
  pip install matplotlib
  pip install numpy
  pip install scipy
  pip freeze > requirements.txt
fi
