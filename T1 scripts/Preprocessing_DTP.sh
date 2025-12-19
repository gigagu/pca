BASE_DATA_PATH=/opt/spark/s3_wrapper/preproc/dtp
echo "Ravi.. Inside Preprocessing_DTP.sh"
COUNTRY=$1
RUN_DATE=`echo $2 | sed 's/_//g' | sed 's/-//g' `
country_u=`echo $COUNTRY | tr '[:lower:]' '[:upper:]'`

country_l=`echo $COUNTRY | tr '[:upper:]' '[:lower:]'`

_fcountry_u=`echo "${country_u}" | sed 's/GLB1/G1/;s/GLB2/G2/;s/GLB3/G3/;s/GLB4/G4/;s/GLB5/G5/;s/GLB6/G6/'`

cd ${BASE_DATA_PATH}/${country_l}/incoming_temp

echo "Ravi.. Before not VN value comparision"
if [ ${country_u} != 'VN' ]
        then
for file_name in `ls ${BASE_DATA_PATH}/${country_l}/incoming_temp/*_${RUN_DATE}_D*`
do
echo "Ravi.. in for loop"
#dos2unix $file_name
sed -i 's/\r$//' ${file_name}

  echo "File Found -> $file_name"
  
  sed -i 's///g ; s///g; s///g' ${file_name}
  sed -i -e 's/ *|/|/g' ${file_name}
  
  if [ ${file_name} == ${BASE_DATA_PATH}/${country_l}/incoming_temp/${_fcountry_u}_DTP_TBLDATA_${RUN_DATE}_D_1.dat ] || [ ${file_name} == ${BASE_DATA_PATH}/${country_l}/incoming_temp/${_fcountry_u}_DTP_TBLGLDATA_${RUN_DATE}_D_1.dat ]
  then
    echo "Ravi.. inside if condition"
    cd ${BASE_DATA_PATH}/${country_l}/incoming_temp
		sed -i 's/|//g' ${file_name}
		awk '{if (/^H/)print >> "H1.txt"; else if (/^T/)print >> "T1.txt";}' ${file_name}
		H=$(< ${BASE_DATA_PATH}/${country_l}/incoming_temp/H1.txt)
        T=$(< ${BASE_DATA_PATH}/${country_l}/incoming_temp/T1.txt)
        sed -i '1d;$d' ${file_name}
		sed -i 's//\n/g5; s/\n/|/g' ${file_name}
		sed -i 's/$//' ${file_name}
        HEAD=`echo ${H//[[:blank:]]/}`
        sed -i '1i '${HEAD}'' ${file_name}
        sed -i '$a '${T}'' ${file_name}
		rm -f ${BASE_DATA_PATH}/${country_l}/incoming_temp/H1.txt
        rm -f ${BASE_DATA_PATH}/${country_l}/incoming_temp/T1.txt

 else
		echo "Ravi.. inside else condition"
    cd ${BASE_DATA_PATH}/${country_l}/incoming_temp
		sed -i 's/|//g' ${file_name}

		sed  -i '/^D/s/$//' ${file_name}
		
		#sed -i 's/$//' ${file_name}

 fi
done
fi

echo "Ravi.. for VN value comparision" 
if [ ${country_u} == 'VN' ]
then
for file_name in `ls $BASE_DATA_PATH/$country_l/incoming_temp/*_${RUN_DATE}_D* `
do
  #dos2unix $file_name
  sed -i 's/\r$//' "$file_name"
  echo "File Found -> $file_name"

  sed -i 's///g ; s///g; s///g' ${file_name}
#  sed -i -e 's/ *|/|/g' ${file_name}
  echo "Ravi.. before VN if condition"
  if [ ${file_name} == ${BASE_DATA_PATH}/${country_l}/incoming_temp/${_fcountry_u}_DTP_TBLGLDATA_${RUN_DATE}_D_1.dat ]
  then
      sed -i -e 's/ *|/|/g' ${file_name}
      sed -i 's/|//g' ${file_name}
      awk '{if (/^H/)print >> "H1.txt"; else if (/^T/)print >> "T1.txt";}' ${file_name}
      H=$(< ${BASE_DATA_PATH}/${country_l}/incoming_temp/H1.txt)
      T=$(< ${BASE_DATA_PATH}/${country_l}/incoming_temp/T1.txt)
      sed -i '1d;$d' ${file_name}
      sed -i 's//\n/g5; s/\n/|/g' ${file_name}
      sed -i '1i '${H}'' ${file_name}
      sed -i '$a '${T}'' ${file_name}
      rm -f ${BASE_DATA_PATH}/${country_l}/incoming_temp/H1.txt
      rm -f ${BASE_DATA_PATH}/${country_l}/incoming_temp/T1.txt

  elif [ ${file_name} == ${BASE_DATA_PATH}/${country_l}/incoming_temp/${_fcountry_u}_DTP_TBLDATA_${RUN_DATE}_D_1.dat ]
  then
    echo "Ravi.. inside elif condition"
    sed -i -e 's/ *!/!/g' ${file_name}
	sed -i 's/!//g' ${file_name}
	awk '{if (/^H/)print >> "H2.txt"; else if (/^T/)print >> "T2.txt";}' ${file_name}
        H=$(< ${BASE_DATA_PATH}/${country_l}/incoming_temp/H2.txt)
        T=$(< ${BASE_DATA_PATH}/${country_l}/incoming_temp/T2.txt)
        sed -i '1d;$d' ${file_name}
        sed -i 's//\n/g5; s/\n/|/g' ${file_name}
        sed -i '1i '${H}'' ${file_name}
        sed -i '$a '${T}'' ${file_name}
        rm -f ${BASE_DATA_PATH}/${country_l}/incoming_temp/H2.txt
        rm -f ${BASE_DATA_PATH}/${country_l}/incoming_temp/T2.txt

  elif [ ${file_name} == ${BASE_DATA_PATH}/${country_l}/incoming_temp/${_fcountry_u}_DTP_CUSTCNA_${RUN_DATE}_D_1.dat ] || [ ${file_name} == ${BASE_DATA_PATH}/${country_l}/incoming_temp/${_fcountry_u}_DTP_PARTYGLROT_${RUN_DATE}_D_1.dat ] || [ ${file_name} == ${BASE_DATA_PATH}/${country_l}/incoming_temp/${_fcountry_u}_DTP_PARTYGLEXT_${RUN_DATE}_D_1.dat ] || [ ${file_name} == ${BASE_DATA_PATH}/${country_l}/incoming_temp/${_fcountry_u}_DTP_TBLMETA_${RUN_DATE}_D_1.dat ] || [ ${file_name} == ${BASE_DATA_PATH}/${country_l}/incoming_temp/${_fcountry_u}_DTP_DEALT05_${RUN_DATE}_D_1.dat ]
  then
        echo "Ravi.. inside 2nd elif condition"
        sed -i -e 's/ *!/!/g' ${file_name}
        sed -i 's/!//g' ${file_name}
  else
        echo "Ravi.. inside else condition of VN"
        sed -i -e 's/ *|/|/g' ${file_name}
		sed -i 's/|//g' ${file_name}
  fi
done
fi
