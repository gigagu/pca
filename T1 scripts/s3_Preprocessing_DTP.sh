#!/bin/bash

source /opt/spark/s3_wrapper/call_CmdExecutor.sh

BASE_DATA_PATH=s3a://gdp/gdp-batch-ingestion/dtp-dev/test-run
echo "Ravi.. Inside Preprocessing_DTP.sh"
COUNTRY=$1
RUN_DATE=`echo $2 | sed 's/_//g' | sed 's/-//g'`
country_u=`echo $COUNTRY | tr '[:lower:]' '[:upper:]'`
country_l=`echo $COUNTRY | tr '[:upper:]' '[:lower:]'`
_fcountry_u=`echo "${country_u}" | sed 's/GLB1/G1/;s/GLB2/G2/;s/GLB3/G3/;s/GLB4/G4/;s/GLB5/G5/;s/GLB6/G6/'`

INCOMING_TEMP_DIR=${BASE_DATA_PATH}/${country_l}/incoming_temp

echo "Ravi.. Before not VN value comparison"
if [ ${country_u} != 'VN' ]; then
    for file_name in $(ls_s3 ${INCOMING_TEMP_DIR}/*_${RUN_DATE}_D*); do
        echo "Ravi.. in for loop"
        sed_s3 -i 's/\r$//' ${file_name}

        echo "File Found -> $file_name"

        sed_s3 -i 's///g ; s///g; s///g' ${file_name}
        sed_s3 -i -e 's/ *|/|/g' ${file_name}

        if [ ${file_name} == ${INCOMING_TEMP_DIR}/${_fcountry_u}_DTP_TBLDATA_${RUN_DATE}_D_1.dat ] || \
           [ ${file_name} == ${INCOMING_TEMP_DIR}/${_fcountry_u}_DTP_TBLGLDATA_${RUN_DATE}_D_1.dat ]; then
            echo "Ravi.. inside if condition"
            sed_s3 -i 's/|//g' ${file_name}
            awk_s3 '{if (/^H/)print >> "H1.txt"; else if (/^T/)print >> "T1.txt";}' ${file_name}
            H=$(cat_s3 ${INCOMING_TEMP_DIR}/H1.txt)
            T=$(cat_s3 ${INCOMING_TEMP_DIR}/T1.txt)
            sed_s3 -i '1d;$d' ${file_name}
            sed_s3 -i 's//\n/g5; s/\n/|/g' ${file_name}
            sed_s3 -i 's/$//' ${file_name}
            HEAD=`echo ${H//[[:blank:]]/}`
            sed_s3 -i "1i ${HEAD}" ${file_name}
            sed_s3 -i "\$a ${T}" ${file_name}
            rm_s3 ${INCOMING_TEMP_DIR}/H1.txt
            rm_s3 ${INCOMING_TEMP_DIR}/T1.txt
        else
            echo "Ravi.. inside else condition"
            sed_s3 -i 's/|//g' ${file_name}
            sed_s3 -i '/^D/s/$//' ${file_name}
        fi
    done
fi

echo "Ravi.. for VN value comparison"
if [ ${country_u} == 'VN' ]; then
    for file_name in $(ls_s3 ${INCOMING_TEMP_DIR}/*_${RUN_DATE}_D*); do
        sed_s3 -i 's/\r$//' ${file_name}
        echo "File Found -> $file_name"

        sed_s3 -i 's///g ; s///g; s///g' ${file_name}

        echo "Ravi.. before VN if condition"
        if [ ${file_name} == ${INCOMING_TEMP_DIR}/${_fcountry_u}_DTP_TBLGLDATA_${RUN_DATE}_D_1.dat ]; then
            sed_s3 -i -e 's/ *|/|/g' ${file_name}
            sed_s3 -i 's/|//g' ${file_name}
            awk_s3 '{if (/^H/)print >> "H1.txt"; else if (/^T/)print >> "T1.txt";}' ${file_name}
            H=$(cat_s3 ${INCOMING_TEMP_DIR}/H1.txt)
            T=$(cat_s3 ${INCOMING_TEMP_DIR}/T1.txt)
            sed_s3 -i '1d;$d' ${file_name}
            sed_s3 -i 's//\n/g5; s/\n/|/g' ${file_name}
            sed_s3 -i "1i ${H}" ${file_name}
            sed_s3 -i "\$a ${T}" ${file_name}
            rm_s3 ${INCOMING_TEMP_DIR}/H1.txt
            rm_s3 ${INCOMING_TEMP_DIR}/T1.txt
        elif [ ${file_name} == ${INCOMING_TEMP_DIR}/${_fcountry_u}_DTP_TBLDATA_${RUN_DATE}_D_1.dat ]; then
            echo "Ravi.. inside elif condition"
            sed_s3 -i -e 's/ *!/!/g' ${file_name}
            sed_s3 -i 's/!//g' ${file_name}
            awk_s3 '{if (/^H/)print >> "H2.txt"; else if (/^T/)print >> "T2.txt";}' ${file_name}
            H=$(cat_s3 ${INCOMING_TEMP_DIR}/H2.txt)
            T=$(cat_s3 ${INCOMING_TEMP_DIR}/T2.txt)
            sed_s3 -i '1d;$d' ${file_name}
            sed_s3 -i 's//\n/g5; s/\n/|/g' ${file_name}
            sed_s3 -i "1i ${H}" ${file_name}
            sed_s3 -i "\$a ${T}" ${file_name}
            rm_s3 ${INCOMING_TEMP_DIR}/H2.txt
            rm_s3 ${INCOMING_TEMP_DIR}/T2.txt
        elif [ ${file_name} == ${INCOMING_TEMP_DIR}/${_fcountry_u}_DTP_CUSTCNA_${RUN_DATE}_D_1.dat ] || \
             [ ${file_name} == ${INCOMING_TEMP_DIR}/${_fcountry_u}_DTP_PARTYGLROT_${RUN_DATE}_D_1.dat ] || \
             [ ${file_name} == ${INCOMING_TEMP_DIR}/${_fcountry_u}_DTP_PARTYGLEXT_${RUN_DATE}_D_1.dat ] || \
             [ ${file_name} == ${INCOMING_TEMP_DIR}/${_fcountry_u}_DTP_TBLMETA_${RUN_DATE}_D_1.dat ] || \
             [ ${file_name} == ${INCOMING_TEMP_DIR}/${_fcountry_u}_DTP_DEALT05_${RUN_DATE}_D_1.dat ]; then
            echo "Ravi.. inside 2nd elif condition"
            sed_s3 -i -e 's/ *!/!/g' ${file_name}
            sed_s3 -i 's/!//g' ${file_name}
        else
            echo "Ravi.. inside else condition of VN"
            sed_s3 -i -e 's/ *|/|/g' ${file_name}
            sed_s3 -i 's/|//g' ${file_name}
        fi
    done
fi