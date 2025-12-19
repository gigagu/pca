
#!/bin/bash

start_time=$(date +%s)

echo "Staring download from SS3.."
export JAVA_TOOL_OPTIONS="-Dcom.amazonaws.sdk.disableCertChecking=true"
export S3_BUCKET_ACCESSKEY=$S3_BUCKET_ACCESSKEY
export S3_BUCKET_SECRETKEY=$S3_BUCKET_SECRETKEY
export S3_ENDPOINT="https://haasbatchreg.hbrscb.dev.net:9021"

python3 /opt/spark/s3_wrapper/s3_command_executor.py "download s3a://gdp/gdp-batch-ingestion/dtp-dev/test-run/eg/gb-file-upld /opt/spark/s3_wrapper/data"

mv /opt/spark/s3_wrapper/data/EG_DTP_BALDA_20220630_1GB_D_1.dat /opt/spark/s3_wrapper/data/EG_DTP_BALDA_20220630_D_1.dat
cp /opt/spark/s3_wrapper/data/EG_DTP_BALDA_20220630_D_1.dat /opt/spark/s3_wrapper/preproc/dtp/eg/src_incoming

if [ $2 == "eg" ]; then
#Checking the passing parameter status.The parameter should be trigger file sh
if [ $# -ne 3 ];then
        echo "--------------------------------------------------------------------------------------------------------------------------"
        echo "Expected 3 args"
        echo "Usage   : <SYSTEM_NAME> <COUNTRY_NAME> <RUN DATE> "
        echo "Example : dtp ae 20191118"
        echo "--------------------------------------------------------------------------------------------------------------------------"
        exit 1;
fi

_appNameUpper=`echo $1 | tr '[:lower:]' '[:upper:]'`
_appNameLower=`echo $1 | tr '[:upper:]' '[:lower:]'`

_country=$2

_countryNameUpper=`echo $_country | tr '[:lower:]' '[:upper:]'`
_countryNameLower=`echo $_country| tr '[:upper:]' '[:lower:]'`

_fcountry=`echo "${_countryNameUpper}" | sed 's/GLB1/G1/;s/GLB2/G2/;s/GLB3/G3/;s/GLB4/G4/;s/GLB5/G5/;s/GLB6/G6/'`
_sdm_run_date_formated=$(date +%Y%m%d -d "$3")
_sdm_run_date=$(date +%Y-%m-%d -d "$3")
_sdm_run_date_formated_prev=$(date +%Y%m%d -d "$3 - 7 day")

#PROD PATHS
_sdm_trg_path=/opt/spark/s3_wrapper/preproc/dtp
_sdm_base_dir=/opt/spark/s3_wrapper/preproc
#_sdm_nas_path=${_sdm_trg_path}/${_countryNameLower}/tmp_incoming
_sdm_nas_path=/opt/spark/s3_wrapper/preproc/dtp/eg/src_incoming
_sdm_archive_dir=${_sdm_trg_path}/${_countryNameLower}/archival
_sdm_incoming_temp_dir=${_sdm_trg_path}/${_countryNameLower}/incoming_temp
_sdm_incoming_dir=${_sdm_trg_path}/${_countryNameLower}/incoming
#_sdm_pre_incoming_dir=${_sdm_trg_path}/${_countryNameLower}/tmp_incoming
_ctrlm_path=${_sdm_trg_path}/ctrlm
_sdm_appl_dir=${_sdm_trg_path}/${_countryNameLower}/appl

LOGDIR=${_sdm_trg_path}/ctrlm/${_countryNameLower}

appNameUpper=`echo ${_appNameLower} | tr '[:lower:]' '[:upper:]'`
appNameLower=`echo ${_appNameLower} | tr '[:upper:]' '[:lower:]'`

countryNameUpper=`echo ${_country} | tr '[:lower:]' '[:upper:]'`
countryNameLower=`echo ${_country} | tr '[:upper:]' '[:lower:]'`

find ${_sdm_nas_path} -type f -mtime +7 -name '*' -execdir rm -- '{}' \;
find ${LOGDIR} -type f -mtime +30 -name '*' -execdir rm -- '{}' \;

# To move files from preprocessing to incoming_temp

        tmp_file_cnt=`ls ${_sdm_incoming_temp_dir} | wc -l`
		if [ ${tmp_file_cnt} -ge 1 ]
        then
		  rm -rf ${_sdm_incoming_temp_dir}/*
		fi
		
if [ -d $LOGDIR ]; then  
        echo "$LOGDIR is available"
        echo "$LOGDIR is available" |& tee -a $LOG_File
		find ${LOGDIR} -type f -mtime +7 -name '*' -execdir rm -- '{}' \;
else
        echo "$LOGDIRis not available, Creating now"
        echo "$LOGDIRis not available, Creating now" |& tee -a $LOG_File
        mkdir -p $LOGDIR
fi

missedfilestatus=0

a=$(ls ${_sdm_nas_path}/*${_sdm_run_date_formated}* | wc -l)
echo $a
if [ $a -lt 1 ];then
missedfilestatus=1

else

echo "Removing ${countryNameLower}_counterFile.txt in case of files received "
echo "Removing ${countryNameLower}_counterFile.txt in case of files received " |& tee -a $LOG_File
if [ -f $_ctrlm_path/${countryNameLower}_counterFile_${_sdm_run_date_formated}.txt ];then
rm -f $_ctrlm_path/${countryNameLower}_counterFile_${_sdm_run_date_formated}.txt
fi


echo "Script Start Time: `date`" |& tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
echo "Script Start Time: `date`" |& tee -a $LOG_File
#gzip ${_sdm_nas_path}/${_fcountry}_${_appNameUpper}_*_${_sdm_run_date_formated}_D_1.dat
echo "BIF_to_sdm_file_copy.sh started" |& tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
echo "BIF_to_sdm_file_copy.sh started"  |& tee -a $LOG_File
echo "File copy processing for country : ${countryNameUpper}"  |& tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
echo "File copy processing for country : ${countryNameUpper}"   |& tee -a $LOG_File


files_pattern_config_file=${_sdm_trg_path}/${countryNameLower}/appl/${appNameLower}_${countryNameLower}_files_list.txt
if [ -f ${files_pattern_config_file} ]
then
        echo "File pattern config file is present" |& tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
        echo "File pattern config file is present"  |& tee -a $LOG_File
else
        echo "File pattern config file is not present" |& tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
        echo "File pattern config file is not present"  |& tee -a $LOG_File
        exit 9
fi
tot_file_count=$(cat ${_sdm_trg_path}/${countryNameLower}/appl/${appNameLower}_${countryNameLower}_files_list.txt | wc -l)
echo "No of files in List file : ${tot_file_count}"  |& tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
echo "No of files in List file : ${tot_file_count}"   |& tee -a $LOG_File

config_dir=$(dirname ${files_pattern_config_file})
files_list=${config_dir}/ongoing_copy_file.lst

if [ -e ${files_list} ]; then
   rm -f ${files_list}
fi

declare -a FILES_LIST_ARRAY
let i=0

for FILE in `cat ${files_pattern_config_file}`
do
        file_with_date=$(echo $FILE | sed "s/##date##/${_sdm_run_date_formated}/g")
        file_without_ext=`echo $file_with_date | cut -d "." -f 1`
        echo ${file_without_ext} >> ${files_list}
        FILES_LIST_ARRAY[i]="${file_without_ext}"
        ((++i))
done

echo "*****************************************************"
echo "*****************************************************" |& tee -a $LOG_File
echo ${FILES_LIST_ARRAY[@]}
#CHECK ARRAY IS EMPTY OR NOT
if [ ${#FILES_LIST_ARRAY[@]} -le 0 ]
then
        echo "Array is empty,Please check the file pattern config file" |& tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
        echo "Array is empty,Please check the file pattern config file" |& tee -a $LOG_File
        exit 9
fi
RES_COUNT=0
j=0
flag=true
for ((i=0; i<${#FILES_LIST_ARRAY[@]}; i++));
do
        if [ ${RES_COUNT} -lt ${tot_file_count} ]; then
                if [ -e ${_sdm_nas_path}/${FILES_LIST_ARRAY[j]}.dat ]; then
                                echo "File exists in  NAS : ${FILES_LIST_ARRAY[j]}.dat" |& tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
                                echo "File exists in  NAS : ${FILES_LIST_ARRAY[j]}.dat"  |& tee -a $LOG_File
                                (( RES_COUNT++ ))
                                (( j++ ))
                else
                                echo "File ${FILES_LIST_ARRAY[j]}.dat doesn't exist in NAS" |& tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
                                echo "File ${FILES_LIST_ARRAY[j]}.dat doesn't exist in NAS"  |& tee -a $LOG_File
								(( j++ ))
                fi
        fi
done


echo "No of files in temp : ${RES_COUNT}"  |& tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
echo "No of files in temp : ${RES_COUNT}"   |& tee -a $LOG_File

######====================================================

if [ $tot_file_count -eq ${RES_COUNT} ]
then

	echo "File count in NAS matching with SDM file list" |& tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
	echo "File count in NAS matching with SDM file list"  |& tee -a $LOG_File
	echo "Starting to Move files to incoming_temp" 
	echo "Starting to Move files to incoming_temp"  |& tee -a $LOG_File
	cp ${_sdm_nas_path}/${_fcountry}_${_appNameUpper}_*_${_sdm_run_date_formated}_D_1.dat ${_sdm_incoming_temp_dir}
	chmod 775 ${_sdm_incoming_temp_dir}/*
	echo "Moving of files to  incoming_temp has been completed"
	echo "Moving of files to  incoming_temp has been completed"  |& tee -a $LOG_File
	#gunzip ${_sdm_incoming_temp_dir}/${_fcountry}_${_appNameUpper}_*_${_sdm_run_date_formated}_D_1.dat.gz
	chmod 775 ${_sdm_incoming_temp_dir}/*
	echo "Unzip  of files to  incoming_temp has been completed"
	echo "Unzip  of files to  incoming_temp has been completed"  |& tee -a $LOG_File
	echo "Script Start Time: `date` " |& tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
	echo "Script Start Time: `date` "  |& tee -a $LOG_File

	echo "preprocessing.sh started " |& tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
	echo "preprocessing.sh started "  |& tee -a $LOG_File

	echo "File pre-processing for country : ${_countryNameUpper} "  |& tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
	echo "File pre-processing for country : ${_countryNameUpper} "   |& tee -a $LOG_File
	
	#Remove header line for all the files
	#sed -i '/^H|/d' ${_sdm_incoming_temp_dir}/${_countryNameUpper}_${_appNameUpper}_*_${_sdm_run_date_formated}_D_1.dat
	
	echo "Runnning : Preprocessing_DTP.sh ${_countryNameLower} ${_sdm_run_date_formated}"
	echo "Runnning : Preprocessing_DTP.sh ${_countryNameLower} ${_sdm_run_date_formated}"  |& tee -a $LOG_File
	sh ${_ctrlm_path}/Preprocessing_DTP.sh ${_countryNameLower} ${_sdm_run_date_formated}
	
	##Remove extra columns as per config properties
	for FILE in `ls ${_sdm_incoming_temp_dir}/${_fcountry}_${_appNameUpper}_*_${_sdm_run_date_formated}_D_1.dat`
	do
		FileName=`basename $FILE`
		
		file_hdr=`head -1 ${_sdm_incoming_temp_dir}/${FileName}`
		
		sed -i '/^H/d' ${_sdm_incoming_temp_dir}/${FileName}
				
		mv ${_sdm_incoming_temp_dir}/${FileName} ${_sdm_incoming_temp_dir}/${FileName}_validated
		
			
		#file_ptrn=`echo ${FileName} | cut -d"_" -f3`
		file_ptrn1=`echo ${FileName} | cut -d"_" -f1-4 | sed -e 's/_${_sdm_run_date_formated}//'`
		file_ptrn=$(echo ${file_ptrn1} | sed "s/_${_sdm_run_date_formated}//g")
		
		col_cnt1=`grep -i ${file_ptrn} ${_sdm_trg_path}/${_countryNameLower}/configs/col_count.config | cut -d'=' -f2`
		#col_cnt=`echo "${col_cnt1} + 1 " | bc`
		col_cnt=$((col_cnt1 + 1))
		
		cut -d"" -f1-${col_cnt} ${_sdm_incoming_temp_dir}/${FileName}_validated  > ${_sdm_incoming_temp_dir}/${FileName}
		
		sed -i '1i '${file_hdr}'' ${_sdm_incoming_temp_dir}/${FileName}
		
	done


### Create EOD Marker File #####
	

	if [ -f ${_sdm_incoming_temp_dir}/${_fcountry}_${_appNameUpper}_EOD_MARKER_${_sdm_run_date_formated}_D_1.dat ]; then

		rm ${_sdm_incoming_temp_dir}/${_fcountry}_${_appNameUpper}_EOD_MARKER_${_sdm_run_date_formated}_D_1.dat
	fi
	
	chmod 775 ${_sdm_incoming_temp_dir}/${_fcountry}_${_appNameUpper}_*_${_sdm_run_date_formated}_D_1.dat
	
	for FILE in `ls ${_sdm_incoming_temp_dir}/${_fcountry}_${_appNameUpper}_*_${_sdm_run_date_formated}_D_1.dat`
	do
		FileName=`basename $FILE`
		
		cp ${_sdm_incoming_temp_dir}/${FileName} ${_sdm_incoming_dir}/
                chmod 775 ${_sdm_incoming_dir}/* 
		
		sleep 2
	done

	echo "Source Files copied to incoming folder" |& tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
	echo "Source Files copied to incoming folder"  |& tee -a $LOG_File

	no_of_files_sdmtemp=$(ls ${_sdm_incoming_temp_dir}/${_fcountry}_${_appNameUpper}_*_${_sdm_run_date_formated}_D_1.dat | wc -l)
		
		if [ $tot_file_count -eq $no_of_files_sdmtemp ]
		then
			sleep 5
			#sed "s/cty/${_fcountry}/g;s/date1/${_sdm_run_date_formated}/g;s/date2/${_sdm_run_date}/g" ${_ctrlm_path}/${_appNameUpper}_EOD_MARKER_yyyymmdd_D_1.dat > ${_sdm_incoming_temp_dir}/${_fcountry}_${_appNameUpper}_EOD_MARKER_${_sdm_run_date_formated}_D_1.dat
			{
			 echo "HEG${_sdm_run_date_formated}${_sdm_run_date_formated}1"
			 echo "D${_sdm_run_date}"
			 echo "T100"
			} > ${_sdm_incoming_temp_dir}/${_fcountry}_${_appNameUpper}_EOD_MARKER_${_sdm_run_date_formated}_D_1.dat

			echo "EOD Marker File created" |& tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
			echo "EOD Marker File created"  |& tee -a $LOG_File

			chmod 775 ${_sdm_incoming_temp_dir}/${_fcountry}_${_appNameUpper}_*_${_sdm_run_date_formated}_D_1.dat
			
			cp ${_sdm_incoming_temp_dir}/${_fcountry}_${_appNameUpper}_EOD_MARKER_${_sdm_run_date_formated}_D_1.dat ${_sdm_incoming_dir}/
			
                        chmod 775 ${_sdm_incoming_dir}/*
			echo "Script End Time: `date` " |& tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
			echo "Script End Time: `date` "  |& tee -a $LOG_File
			rm -rf ${_sdm_incoming_temp_dir}/*_validated

			upload_start_time=$(date +%s)
			#find /CTRLFW/BCBS/SDM/PRD/dtp/sa/tmp_incoming/* -mtime +20 -exec rm {} \;
			echo "Ravi---At line 287.. Uploading files to S3 bucket"
			for file in /opt/spark/s3_wrapper/preproc/dtp/eg/incoming/*; do
				python3 /opt/spark/s3_wrapper/s3_command_executor.py "upload $file s3a://gdp/gdp-batch-ingestion/dtp-dev/test-run/eg/demo_test/$(basename $file)"
			done
			
			end_time=$(date +%s)
			echo " Upload ended at $end_time"
			echo " Upload time taken: $((end_time - upload_start_time)) seconds"

			total_time=$((end_time - start_time))
			echo "At line 303... Total time taken by the process: ${total_time} seconds" | tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
			exit 0
		else
			upload_start_time=$(date +%s)
			echo "Ravi---At line 299.. Uploading files to S3 bucket"
			for file in /opt/spark/s3_wrapper/preproc/dtp/eg/incoming/*; do
				python3 /opt/spark/s3_wrapper/s3_command_executor.py "upload $file s3a://gdp/gdp-batch-ingestion/dtp-dev/test-run/eg/demo_test/$(basename $file)"
			done
			
			end_time=$(date +%s)
			echo " Upload ended at $end_time"
			echo " Upload time taken: $((end_time - upload_start_time)) seconds"
			
			total_time=$((end_time - start_time))
			echo "At line 308... Total time taken by the process: ${total_time} seconds" | tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
			exit 9
		fi

else
    echo "file count doesnt matches between NAS and SDM list file" |& tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
    end_time=$(date +%s)
    total_time=$((end_time - start_time))
    echo "At line 316... Total time taken by the process: ${total_time} seconds" | tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
    exit 9
fi
fi

### Sending mail to source in case files not received after cut off time ###

if [ ${missedfilestatus} -eq 1 ];then
if [ -f $_ctrlm_path/${countryNameLower}_counterFile_${_sdm_run_date_formated}.txt ];then
vRunCounter=`cat $_ctrlm_path/${countryNameLower}_counterFile_${_sdm_run_date_formated}.txt`
vRunCounter=`expr ${vRunCounter} + 1`
echo ${vRunCounter} > $_ctrlm_path/${countryNameLower}_counterFile_${_sdm_run_date_formated}.txt
if [ ${vRunCounter} -ge 7 ];then
mail -v -s "Files not received at EDMp for "${_sdm_run_date_formated}"" TradeDTP.ProductionEngineering@sc.com,GBLIMEXMFProductionEngineering@sc.com,EDM-PSS-T1@sc.com <<< "EDMp has not received the expected number of files from $SRC $countryNameUpper"
echo "File has not received. sending mail to source team." |& tee -a $LOGDIR/dtp_sdm_file_preprocessing_${countryNameLower}_$_sdm_run_date.log
echo "File has not received. sending mail to source team."  |& tee -a $LOG_File
exit 2
fi
echo "OLA Cut-off time has not reached. hence ingoring and waiting next cycle"
echo "OLA Cut-off time has not reached. hence ingoring and waiting next cycle"  |& tee -a $LOG_File
exit 9

else
vRunCounter=1
echo $vRunCounter > $_ctrlm_path/${countryNameLower}_counterFile_${_sdm_run_date_formated}.txt
echo "OLA Cut-off time has not reached. hence ingoring and waiting next cycle"
exit 9
fi
fi

exit 0

else
#Checking the passing parameter status.The parameter should be trigger file sh
if [ $# -ne 3 ];then
        echo "--------------------------------------------------------------------------------------------------------------------------"
        echo "Expected 3 args"
        echo "Usage   : <SYSTEM_NAME> <COUNTRY_NAME> <RUN DATE> "
        echo "Example : dtp ae 20191118"
        echo "--------------------------------------------------------------------------------------------------------------------------"
        exit 1;
fi

_appNameUpper=`echo $1 | tr '[:lower:]' '[:upper:]'`
_appNameLower=`echo $1 | tr '[:upper:]' '[:lower:]'`

_country=$2

_countryNameUpper=`echo $_country | tr '[:lower:]' '[:upper:]'`
_countryNameLower=`echo $_country| tr '[:upper:]' '[:lower:]'`

_fcountry=`echo "${_countryNameUpper}" | sed 's/GLB1/G1/;s/GLB2/G2/;s/GLB3/G3/;s/GLB4/G4/;s/GLB5/G5/;s/GLB6/G6/'`
_sdm_run_date_formated=$(date +%Y%m%d -d "$3")
_sdm_run_date=$(date +%Y-%m-%d -d "$3")
_sdm_run_date_formated_prev=$(date +%Y%m%d -d "$3 - 7 day")

#PROD PATHS
_sdm_trg_path=/CTRLFW/BCBS/SDM/PRD/dtp
_sdm_base_dir=/CTRLFW/BCBS/SDM/PRD
#_sdm_nas_path=/CTRLFW/BCBS/prd/dtp/data/${_countryNameUpper}/nas/file_backup
_sdm_nas_path=/CTRLFW/BCBS/prd/dtp/data/${_countryNameUpper}/nas
_sdm_archive_dir=${_sdm_trg_path}/${_countryNameLower}/archival
_sdm_incoming_temp_dir=${_sdm_trg_path}/${_countryNameLower}/incoming_temp
_sdm_incoming_dir=${_sdm_trg_path}/${_countryNameLower}/incoming
_sdm_pre_incoming_dir=${_sdm_trg_path}/${_countryNameLower}/tmp_incoming
_ctrlm_path=${_sdm_trg_path}/ctrlm
_sdm_appl_dir=${_sdm_trg_path}/${_countryNameLower}/appl

LOGDIR=${_sdm_trg_path}/ctrlm/${_countryNameLower}


appNameUpper=`echo ${_appNameLower} | tr '[:lower:]' '[:upper:]'`
appNameLower=`echo ${_appNameLower} | tr '[:upper:]' '[:lower:]'`

countryNameUpper=`echo ${_country} | tr '[:lower:]' '[:upper:]'`
countryNameLower=`echo ${_country} | tr '[:upper:]' '[:lower:]'`

#COP LOG CHANGE STARTS
[ -f /CTRLFW/SOURCG/SCUDEE/SDM/edmp-alerts/cop_log.properties ] && base_log_path=`cat /CTRLFW/SOURCG/SCUDEE/SDM/edmp-alerts/cop_log.properties` || base_log_path=/CTRLFW/SOURCG/SCUDEE/SDM/PRD/logs
LOG_DIR=$base_log_path/${_appNameLower}
mkdir -p $LOG_DIR
chmod -R 775 $LOG_DIR
LOG_File=$LOG_DIR/${_appNameLower}_${_countryNameLower}_v2_file_copy_$3.log
echo "Script Start Time: `date`" |& tee -a $LOG_File
chmod 775 $LOG_File
echo "File copy processing for country : ${_countryNameUpper}" |& tee -a $LOG_File
#COP LOG CHANGE ENDS

# To move files from preprocessing to incoming_temp

        tmp_file_cnt=`ls ${_sdm_incoming_temp_dir} | wc -l`
		if [ ${tmp_file_cnt} -ge 1 ]
        then
		  rm -rf ${_sdm_incoming_temp_dir}/*
		fi
		
if [ -d $LOGDIR ]; then  
        echo "$LOGDIR is available"
else
        echo "$LOGDIRis not available, Creating now"
        mkdir -p $LOGDIR
fi

echo "Script Start Time: `date`" |& tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
echo "Script Start Time: `date`"  |& tee -a $LOG_File

echo "BIF_to_sdm_file_copy.sh started" |& tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
echo "BIF_to_sdm_file_copy.sh started"  |& tee -a $LOG_File
echo "File copy processing for country : ${countryNameUpper}"  |& tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
echo "File copy processing for country : ${countryNameUpper}"  |& tee -a $LOG_File


files_pattern_config_file=${_sdm_trg_path}/${countryNameLower}/appl/${appNameLower}_${countryNameLower}_files_list.txt
if [ -f ${files_pattern_config_file} ]
then
        echo "File pattern config file is present" |& tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
        echo "File pattern config file is present"  |& tee -a $LOG_File
else
        echo "File pattern config file is not present" |& tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
        echo "File pattern config file is not present"  |& tee -a $LOG_File
        exit 9
fi
tot_file_count=$(cat ${_sdm_trg_path}/${countryNameLower}/appl/${appNameLower}_${countryNameLower}_files_list.txt | wc -l)
echo "No of files in List file : ${tot_file_count}"  |& tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
echo "No of files in List file : ${tot_file_count}"   |& tee -a $LOG_File

config_dir=$(dirname ${files_pattern_config_file})
files_list=${config_dir}/ongoing_copy_file.lst

if [ -e ${files_list} ]; then
   rm -f ${files_list}
fi

declare -a FILES_LIST_ARRAY
let i=0

for FILE in `cat ${files_pattern_config_file}`
do
        file_with_date=$(echo $FILE | sed "s/##date##/${_sdm_run_date_formated}/g")
        file_without_ext=`echo $file_with_date | cut -d "." -f 1`
        echo ${file_without_ext} >> ${files_list}
        FILES_LIST_ARRAY[i]="${file_without_ext}"
        ((++i))
done

echo "*****************************************************"
echo "*****************************************************" |& tee -a $LOG_File
echo ${FILES_LIST_ARRAY[@]}
#CHECK ARRAY IS EMPTY OR NOT
if [ ${#FILES_LIST_ARRAY[@]} -le 0 ]
then
        echo "Array is empty,Please check the file pattern config file" |& tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
        echo "Array is empty,Please check the file pattern config file"  |& tee -a $LOG_File
        exit 9
fi
RES_COUNT=0
j=0
flag=true
for ((i=0; i<${#FILES_LIST_ARRAY[@]}; i++));
do
        if [ ${RES_COUNT} -lt ${tot_file_count} ]; then
                #if [ -e ${_sdm_nas_path}/${FILES_LIST_ARRAY[j]}.dat.gz ]; then
                if [ -e ${_sdm_nas_path}/${FILES_LIST_ARRAY[j]}.dat ]; then
                                echo "File exists in  NAS : ${FILES_LIST_ARRAY[j]}.dat.gz" |& tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
                                echo "File exists in  NAS : ${FILES_LIST_ARRAY[j]}.dat.gz"  |& tee -a $LOG_File
                                (( RES_COUNT++ ))
                                (( j++ ))
                else
                                echo "File ${FILES_LIST_ARRAY[j]}.dat.gz doesn't exist in NAS" |& tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
                                echo "File ${FILES_LIST_ARRAY[j]}.dat.gz doesn't exist in NAS"  |& tee -a $LOG_File
								(( j++ ))
                fi
        fi
done


echo "No of files in temp : ${RES_COUNT}"  |& tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
echo "No of files in temp : ${RES_COUNT}"   |& tee -a $LOG_File

######====================================================

if [ $tot_file_count -eq ${RES_COUNT} ]
then

	echo "File count in NAS matching with SDM file list" |& tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
	echo "File count in NAS matching with SDM file list"  |& tee -a $LOG_File
	echo "Starting to Move files to incoming_temp" 
	echo "Starting to Move files to incoming_temp"  |& tee -a $LOG_File
	#cp ${_sdm_nas_path}/${_fcountry}_${_appNameUpper}_*_${_sdm_run_date_formated}_D_1.dat.gz ${_sdm_incoming_temp_dir}
	cp ${_sdm_nas_path}/${_fcountry}_${_appNameUpper}_*_${_sdm_run_date_formated}_D_1.dat ${_sdm_incoming_temp_dir}
	chmod 775 ${_sdm_incoming_temp_dir}/*
	echo "Moving of files to  incoming_temp has been completed"
	echo "Moving of files to  incoming_temp has been completed"  |& tee -a $LOG_File
	#gunzip ${_sdm_incoming_temp_dir}/${_fcountry}_${_appNameUpper}_*_${_sdm_run_date_formated}_D_1.dat.gz
	#chmod 775 ${_sdm_incoming_temp_dir}/*
	echo "Unzip  of files to  incoming_temp has been completed"
	echo "Unzip  of files to  incoming_temp has been completed"  |& tee -a $LOG_File
	echo "Script Start Time: `date` " |& tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
	echo "Script Start Time: `date` " |& tee -a $LOG_File

	echo "preprocessing.sh started " |& tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
	echo "preprocessing.sh started "  |& tee -a $LOG_File

	echo "File pre-processing for country : ${_countryNameUpper} "  |& tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
	echo "File pre-processing for country : ${_countryNameUpper} "   |& tee -a $LOG_File
	
	#Remove header line for all the files
	#sed -i '/^H|/d' ${_sdm_incoming_temp_dir}/${_countryNameUpper}_${_appNameUpper}_*_${_sdm_run_date_formated}_D_1.dat
	
	echo "Runnning : Preprocessing_DTP.sh ${_countryNameLower} ${_sdm_run_date_formated}"
	echo "Runnning : Preprocessing_DTP.sh ${_countryNameLower} ${_sdm_run_date_formated}"  |& tee -a $LOG_File
	sh ${_ctrlm_path}/Preprocessing_DTP.sh ${_countryNameLower} ${_sdm_run_date_formated}
	
	##Remove extra columns as per config properties
	for FILE in `ls ${_sdm_incoming_temp_dir}/${_fcountry}_${_appNameUpper}_*_${_sdm_run_date_formated}_D_1.dat`
	do
		FileName=`basename $FILE`
		
		file_hdr=`head -1 ${_sdm_incoming_temp_dir}/${FileName}`
		
		sed -i '/^H/d' ${_sdm_incoming_temp_dir}/${FileName}
				
		mv ${_sdm_incoming_temp_dir}/${FileName} ${_sdm_incoming_temp_dir}/${FileName}_validated
		
		echo "Ravi.. Processing file : ${FileName}"	
		#file_ptrn=`echo ${FileName} | cut -d"_" -f3`
		file_ptrn1=`echo ${FileName} | cut -d"_" -f1-4 | sed -e 's/_${_sdm_run_date_formated}//'`
		file_ptrn=$(echo ${file_ptrn1} | sed "s/_${_sdm_run_date_formated}//g")
		
		col_cnt1=`grep -i ${file_ptrn} ${_sdm_trg_path}/${_countryNameLower}/configs/col_count.config | cut -d'=' -f2`
		#col_cnt=`echo "${col_cnt1} + 1 " | bc`
		col_cnt=$((col_cnt1 + 1))
		
		echo "Ravi.. Column count for ${file_ptrn} is : ${col_cnt}"
		cut -d"" -f1-${col_cnt} ${_sdm_incoming_temp_dir}/${FileName}_validated  > ${_sdm_incoming_temp_dir}/${FileName}
		echo "Ravi.. After cut"
		sed -i '1i '${file_hdr}'' ${_sdm_incoming_temp_dir}/${FileName}
		
	done


### Create EOD Marker File #####
	echo "Ravi.. Creating EOD Marker File"
	if [ -f ${_sdm_incoming_temp_dir}/${_fcountry}_${_appNameUpper}_EOD_MARKER_${_sdm_run_date_formated}_D_1.dat ]; then

		rm ${_sdm_incoming_temp_dir}/${_fcountry}_${_appNameUpper}_EOD_MARKER_${_sdm_run_date_formated}_D_1.dat
	fi
	
	chmod 775 ${_sdm_incoming_temp_dir}/${_fcountry}_${_appNameUpper}_*_${_sdm_run_date_formated}_D_1.dat
	echo "Ravi.. EOD Marker File created"
	for FILE in `ls ${_sdm_incoming_temp_dir}/${_fcountry}_${_appNameUpper}_*_${_sdm_run_date_formated}_D_1.dat`
	do
		echo "Ravi.. Copying source files to incoming folder"
		FileName=`basename $FILE`
		
		cp ${_sdm_incoming_temp_dir}/${FileName} ${_sdm_incoming_dir}/
                chmod 775 ${_sdm_incoming_dir}/* 
		
		sleep 3
	done

	echo "Source Files copied to incoming folder" |& tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
	echo "Source Files copied to incoming folder"  |& tee -a $LOG_File

	no_of_files_sdmtemp=$(ls ${_sdm_incoming_temp_dir}/${_fcountry}_${_appNameUpper}_*_${_sdm_run_date_formated}_D_1.dat | wc -l)

		if [ $tot_file_count -eq $no_of_files_sdmtemp ]
		then

			sed "s/cty/${_fcountry}/g;s/date1/${_sdm_run_date_formated}/g;s/date2/${_sdm_run_date}/g" ${_ctrlm_path}/${_appNameUpper}_EOD_MARKER_yyyymmdd_D_1.dat > ${_sdm_incoming_temp_dir}/${_fcountry}_${_appNameUpper}_EOD_MARKER_${_sdm_run_date_formated}_D_1.dat

			echo "EOD Marker File created" |& tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
			echo "EOD Marker File created"  |& tee -a $LOG_File

			chmod 775 ${_sdm_incoming_temp_dir}/${_fcountry}_${_appNameUpper}_*_${_sdm_run_date_formated}_D_1.dat
			
			cp ${_sdm_incoming_temp_dir}/${_fcountry}_${_appNameUpper}_EOD_MARKER_${_sdm_run_date_formated}_D_1.dat ${_sdm_incoming_dir}/
			
                        chmod 775 ${_sdm_incoming_dir}/*
			echo "Script End Time: `date` " |& tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
			echo "Script End Time: `date` "  |& tee -a $LOG_File
			rm -rf ${_sdm_incoming_temp_dir}/*_validated
			find ${_sdm_archive_dir} -type f -name '*DTP_*_D_1.da*' -mtime +10 -exec rm -f {} \;
			find ${_sdm_nas_path}/* -mtime +10 -exec rm -f {} \;
			find ${LOGDIR}/* -mtime +7 -exec rm -f {} \;

			echo "Ravi---At line 598.. Uploading files to S3 bucket"

			upload_start_time=$(date +%s)
			echo " Upload started at $upload_start_time and file size is $(du -sh /opt/spark/s3_wrapper/preproc/dtp/eg/incoming/EG_DTP_BALDA_20220630_D_1.dat | awk '{print $1}')"

			for file in /opt/spark/s3_wrapper/preproc/dtp/eg/incoming/*; do
				python3 /opt/spark/s3_wrapper/s3_command_executor.py "upload $file s3a://gdp/gdp-batch-ingestion/dtp-dev/test-run/eg/test/test2/$(basename $file)"
			done

			end_time=$(date +%s)
			echo " Upload ended at $end_time"
			echo " Upload time taken: $((end_time - upload_start_time)) seconds"
			total_time=$((end_time - start_time))
			echo "At line 609... Total time taken by the process: ${total_time} seconds" | tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
			exit 0
		else
			
	echo "Ravi---At line 598.. Uploading files to S3 bucket"

			upload_start_time=$(date +%s)
			echo " Upload started at $upload_start_time and file size is $(du -sh /opt/spark/s3_wrapper/preproc/dtp/eg/incoming/EG_DTP_BALDA_20220630_D_1.dat | awk '{print $1}')"

			upload_to_s3 "/opt/spark/s3_wrapper/preproc/dtp/eg/incoming/EG_DTP_BALDA_20220630_D_1.dat" "s3a://gdp/gdp-batch-ingestion/dtp-dev/test-run/eg/test/EG_DTP_BALDA_20220630_D_1.dat"
			
			end_time=$(date +%s)
			echo " Upload ended at $end_time"
			echo " Upload time taken: $((end_time - upload_start_time)) seconds"
			total_time=$((end_time - start_time))
			echo "At line 617... Total time taken by the process: ${total_time} seconds" | tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
			exit 9
		fi

else
    echo "file count doesnt matches between NAS and SDM list file" |& tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
    end_time=$(date +%s)
    total_time=$((end_time - start_time))
    echo "At line 625... Total time taken by the process: ${total_time} seconds" | tee -a $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log
    exit 9
fi
fi
