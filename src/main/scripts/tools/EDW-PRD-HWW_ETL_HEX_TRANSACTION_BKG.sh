#!/bin/bash

cd /usr/etl/HWW;
 
sh /usr/etl/HWW/hdp_hww_hex_etl/tools/etldm_hcom_bkg_order_xref_hex.sh
ERROR_CODE=$? 
echo $ERROR_CODE
if [ $ERROR_CODE -eq 255 ]; then
  exit 0
elif [ $ERROR_CODE -eq 1 ]; then
  exit 1 
fi;
 
sudo -E -u hwwetl /usr/etl/HWW/hdp_hww_hex_etl/LNX-HCOM_HEX_TRANSACTION_BKG.sh

