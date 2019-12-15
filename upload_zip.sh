#!/bin/bash
proj=artificer-transform-materials
aws lambda update-function-code --function-name $proj --zip-file fileb://$proj.zip
