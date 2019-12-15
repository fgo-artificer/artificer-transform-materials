#!/bin/bash
libs='boto3'
rm -rf ./package
python3.8 -m pip install --system --target ./package $libs
# sudo apt install zip
proj=artificer-transform-materials
rm -rf $proj.zip
cd package
zip -r9 ../$proj.zip .
cd ..
zip -g $proj.zip $proj.py
