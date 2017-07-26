#!/bin/sh
# service apache2 restart

$OpenID = $1
$APPID = $2
$AccessToken = $3
$StartTime = $4
$Username = $5
$FILE_PREFIX = $6

$py = ${FILE_PREFIX}"history_basic.py"

sudo bash

python $py $OpenID $APPID $AccessToken $StartTime $Username $FILE_PREFIX