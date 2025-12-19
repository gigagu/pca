#!/bin/bash

# Suppress InsecureRequestWarning
export PYTHONWARNINGS="ignore:Unverified HTTPS request"

start_time=$(date +%s)

source /opt/spark/s3_wrapper/call_CmdExecutor.sh

echo "Starting the script execution at: $(date)" | tee -a /tmp/script_execution.log

# Validate arguments
if [ $# -ne 3 ]; then
    echo "--------------------------------------------------------------------------------------------------------------------------"
    echo "Expected 3 args"
    echo "Usage   : <SYSTEM_NAME> <COUNTRY_NAME> <RUN DATE>"
    echo "Example : dtp ae 20191118"
    echo "--------------------------------------------------------------------------------------------------------------------------"
    exit 1
fi

_appNameUpper=`echo $1 | tr '[:lower:]' '[:upper:]'`
_appNameLower=`echo $1 | tr '[:upper:]' '[:lower:]'`

_country=$2

_countryNameUpper=`echo $_country | tr '[:lower:]' '[:upper:]'`
_countryNameLower=`echo $_country| tr '[:upper:]' '[:lower:]'`

echo "At line 30... before calling sed_s3.."

_fcountry=`echo "${_countryNameUpper}" | sed_s3 's/GLB1/G1/;s/GLB2/G2/;s/GLB3/G3/;s/GLB4/G4/;s/GLB5/G5/;s/GLB6/G6/'`
_sdm_run_date_formated=$(date +%Y%m%d -d "$3")
_sdm_run_date=$(date +%Y-%m-%d -d "$3")
_sdm_run_date_formated_prev=$(date +%Y%m%d -d "$3 - 7 day")

#PROD PATHS
_sdm_trg_path=s3a://gdp/gdp-batch-ingestion/dtp-dev/test-run
_sdm_base_dir=s3a://gdp/gdp-batch-ingestion/dtp-dev
_sdm_nas_path=${_sdm_trg_path}/${_countryNameLower}/GBDemo/src_incoming
_sdm_archive_dir=${_sdm_trg_path}/${_countryNameLower}/GBDemo/archival
_sdm_incoming_temp_dir=${_sdm_trg_path}/${_countryNameLower}/GBDemo/incoming_temp
_sdm_incoming_dir=${_sdm_trg_path}/${_countryNameLower}/GBDemo/incoming
_ctrlm_path=${_sdm_trg_path}/ctrlm
_local_ctrlm_path=/opt/spark/s3_wrapper/preproc/dtp/ctrlm
_sdm_appl_dir=${_sdm_trg_path}/${_countryNameLower}/appl

LOGDIR=${_sdm_trg_path}/ctrlm/${_countryNameLower}

appNameUpper=`echo ${_appNameLower} | tr '[:lower:]' '[:upper:]'`
appNameLower=`echo ${_appNameLower} | tr '[:upper:]' '[:lower:]'`

countryNameUpper=`echo ${_country} | tr '[:lower:]' '[:upper:]'`
countryNameLower=`echo ${_country} | tr '[:upper:]' '[:lower:]'`

# Log the start of the script
echo "Script execution started at: $(date)" | tee -a /tmp/script_execution.log
tee_s3 "$LOGDIR/script_execution.log" "Script execution started at: $(date)"

echo "Arguments: SYSTEM_NAME=$1, COUNTRY_NAME=$2, RUN_DATE=$3" | tee -a /tmp/script_execution.log
tee_s3 "$LOGDIR/script_execution.log" "Arguments: SYSTEM_NAME=$1, COUNTRY_NAME=$2, RUN_DATE=$3"

# Validate critical variables
if [ -z "$_sdm_trg_path" ]; then
    echo "Error: _sdm_trg_path is not set." | tee -a /tmp/script_execution.log
    tee_s3 "$LOGDIR/script_execution.log" "Error: _sdm_trg_path is not set."
    exit 1
fi

if [ -z "$LOGDIR" ]; then
    echo "Error: LOGDIR is not set." | tee -a /tmp/script_execution.log
    tee_s3 "$LOGDIR/script_execution.log" "Error: LOGDIR is not set."
    exit 1
fi

if [ -z "$_countryNameLower" ]; then
    echo "Error: _countryNameLower is not set." | tee -a /tmp/script_execution.log
    tee_s3 "$LOGDIR/script_execution.log" "Error: _countryNameLower is not set."
    exit 1
fi

# Move LOG_File initialization earlier
LOG_File=$LOGDIR/${_appNameLower}_${_countryNameLower}_v2_file_copy_$3.log

# Ensure LOGDIR exists before using LOG_File
if ! mkdir_s3 "$LOGDIR"; then
    echo "Debug: mkdir_s3 failed for LOGDIR=$LOGDIR" | tee -a /tmp/script_execution.log
    tee_s3 "$LOGDIR/script_execution.log" "Debug: mkdir_s3 failed for LOGDIR=$LOGDIR"

    echo "Failed to create LOGDIR: $LOGDIR" | tee -a /tmp/script_execution.log
    tee_s3 "$LOGDIR/script_execution.log" "Failed to create LOGDIR: $LOGDIR"
    exit 1
else
    echo "Debug: mkdir_s3 succeeded for LOGDIR=$LOGDIR" | tee -a /tmp/script_execution.log
    tee_s3 "$LOGDIR/script_execution.log" "Debug: mkdir_s3 succeeded for LOGDIR=$LOGDIR"
fi

# Debug log for tee_s3
echo "Debug: Testing tee_s3" | tee -a /tmp/script_execution.log
if ! tee_s3 "$LOGDIR/script_execution.log" "Debug: Testing tee_s3"; then
    echo "Debug: tee_s3 failed for LOGDIR/script_execution.log" | tee -a /tmp/script_execution.log
    tee_s3 "$LOGDIR/script_execution.log" "Debug: tee_s3 failed for LOGDIR/script_execution.log"
    exit 1
else
    echo "Debug: tee_s3 succeeded for LOGDIR/script_execution.log" | tee -a /tmp/script_execution.log
    tee_s3 "$LOGDIR/script_execution.log" "Debug: tee_s3 succeeded for LOGDIR/script_execution.log"
fi

# Validate LOG_File variable
if [ -z "$LOG_File" ]; then
    echo "Error: LOG_File is not set." | tee -a /tmp/script_execution.log
    tee_s3 "$LOGDIR/script_execution.log" "Error: LOG_File is not set."
    exit 1
fi

echo "Debug: LOGDIR is set to $LOGDIR" | tee -a /tmp/script_execution.log
tee_s3 "$LOGDIR/script_execution.log" "Debug: LOGDIR is set to $LOGDIR"

echo "Debug: LOG_File is set to $LOG_File" | tee -a /tmp/script_execution.log
tee_s3 "$LOGDIR/script_execution.log" "Debug: LOG_File is set to $LOG_File"

# Cleanup old files
cleanup_old_s3 "$_sdm_nas_path" 7 || {
    echo "Failed to clean $_sdm_nas_path" | tee -a /tmp/script_execution.log
    tee_s3 "$LOG_File" "Failed to clean $_sdm_nas_path"
    exit 1
}

cleanup_old_s3 "$LOGDIR" 30 || {
    echo "Failed to clean $LOGDIR" | tee -a /tmp/script_execution.log
    tee_s3 "$LOG_File" "Failed to clean $LOGDIR"
    exit 1
}

# Check and clean incoming_temp directory
tmp_file_cnt=$(ls_s3 "$_sdm_incoming_temp_dir" | wc -l)
if [ "$tmp_file_cnt" -ge 1 ]; then
    rm_s3 "${_sdm_incoming_temp_dir}/*" || {
        echo "Failed to clean $_sdm_incoming_temp_dir" | tee -a /tmp/script_execution.log
        tee_s3 "$LOG_File" "Failed to clean $_sdm_incoming_temp_dir"
        exit 1
    }
fi

# Validate file pattern config file
files_pattern_config_file=${_sdm_trg_path}/${_countryNameLower}/appl/${_appNameLower}_${_countryNameLower}_files_list.txt
if ! ls_s3 "$files_pattern_config_file" > /dev/null 2>&1; then
    echo "File pattern config file is not present" | tee -a /tmp/script_execution.log
    tee_s3 "$LOG_File" "File pattern config file is not present"
    exit 9
fi

# Read file list and validate
echo "Ravi.. testing at line 152..." | tee -a /tmp/script_execution.log
cat_s3 "$files_pattern_config_file" | cat -n
echo "$(cat_s3 "$files_pattern_config_file")" | tee -a /tmp/script_execution.log
echo "At line 153... Debug: Reading file list from $files_pattern_config_file" | tee -a /tmp/script_execution.log
tot_file_count=$(cat_s3 "$files_pattern_config_file" | tr -d '\r' | grep -v '^$' | wc -l)
echo "At line 154... total file count is $tot_file_count" | tee -a /tmp/script_execution.log
if [ "$tot_file_count" -le 0 ]; then
    echo "File list is empty. Please check the file pattern config file." | tee -a /tmp/script_execution.log
    tee_s3 "$LOG_File" "File list is empty. Please check the file pattern config file."
    exit 9
fi

declare -a FILES_LIST_ARRAY
i=0

for FILE in $(cat_s3 "$files_pattern_config_file"); do
    echo "Ravi.. at line 163... file is $FILE" | tee -a /tmp/script_execution.log
    file_with_date=$(echo "$FILE" | sed_s3 "s/##date##/${_sdm_run_date_formated}/g") || {
    echo "sed command failed for $FILE" | tee -a /tmp/script_execution.log
        tee_s3 "$LOG_File" "sed command failed for $FILE"
        exit 1
    }
    FILES_LIST_ARRAY[i]="$file_with_date"
    ((++i))
done

# Validate files in NAS
RES_COUNT=0
for FILE in "${FILES_LIST_ARRAY[@]}"; do
    if ls_s3 "${_sdm_nas_path}/${FILE}" > /dev/null 2>&1; then
        ((RES_COUNT++))
    else
    echo "File not found in NAS: $FILE" | tee -a /tmp/script_execution.log
        tee_s3 "$LOG_File" "File not found in NAS: $FILE"
    fi
done

echo "File count in NAS: $RES_COUNT" | tee -a /tmp/script_execution.log
echo "Total expected file count: $tot_file_count" | tee -a /tmp/script_execution.log
if [ "$RES_COUNT" -ne "$tot_file_count" ]; then
    echo "At line 184" | tee -a /tmp/script_execution.log
    echo "File count mismatch between NAS and SDM list file" | tee -a /tmp/script_execution.log
    tee_s3 "$LOG_File" "File count mismatch between NAS and SDM list file"
    exit 9
fi

# Copy files to incoming_temp
for FILE in "${FILES_LIST_ARRAY[@]}"; do
    # Copy file and validate
    src_path="${_sdm_nas_path}/${FILE}"
    dest_path="${_sdm_incoming_temp_dir}/$(basename "$FILE")"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting copy: $src_path -> $dest_path" >> /tmp/script_execution.log
    cp_s3 "$src_path" "$dest_path"
    rc=$?
    if [ $rc -ne 0 ]; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: Failed to copy $FILE to $dest_path (rc=$rc)" >> /tmp/script_execution.log
        tee_s3 "$LOG_File" "Failed to copy $FILE to $dest_path (rc=$rc)"
        exit 1
    fi
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Completed copy: $src_path -> $dest_path" >> /tmp/script_execution.log
    # Get metadata of copied file
    copied_lines=$(cat_s3 "$dest_path" | wc -l)
    copied_size=$(cat_s3 "$dest_path" | wc -c)
    copied_md5=$(cat_s3 "$dest_path" | md5sum | awk '{print $1}')
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Copied file metadata for $dest_path: lines=$copied_lines, size=${copied_size} bytes, md5=$copied_md5" >> /tmp/script_execution.log
    # Validate copy completeness
    temp_path="$dest_path"
    src_lines=$(cat_s3 "$src_path" | wc -l)
    temp_lines=$(cat_s3 "$temp_path" | wc -l)
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Source file $src_path has $src_lines lines" >> /tmp/script_execution.log
    src_size=$(cat_s3 "$src_path" | wc -c)
    src_md5=$(cat_s3 "$src_path" | md5sum | awk '{print $1}')
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Source file $src_path: lines=$src_lines, size=${src_size} bytes, md5=$src_md5" >> /tmp/script_execution.log
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Copied file $temp_path has $temp_lines lines" >> /tmp/script_execution.log
    temp_size=$(cat_s3 "$temp_path" | wc -c)
    temp_md5=$(cat_s3 "$temp_path" | md5sum | awk '{print $1}')
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Copied file $temp_path: lines=$temp_lines, size=${temp_size} bytes, md5=$temp_md5" >> /tmp/script_execution.log
    if [ "$src_lines" -eq 0 ] || [ "$temp_lines" -eq 0 ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: One of the files is empty after copy: $src_path or $temp_path" >> /tmp/script_execution.log
        exit 1
    fi
    if [ "$src_lines" -ne "$temp_lines" ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: Line count mismatch after copy for $FILE: src=$src_lines, temp=$temp_lines" >> /tmp/script_execution.log
        exit 1
    fi
done

# Process files in incoming_temp
for FILE in $(ls_s3 "${_sdm_incoming_temp_dir}/*"); do
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting processing: $FILE" >> /tmp/script_execution.log
    # Only process files that do NOT end with _validated
    if [[ "$FILE" != *_validated ]]; then
        FileName=$(basename "$FILE")
        file_hdr=$(head_s3 -1 "$FILE")
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] FileName: $FileName" >> /tmp/script_execution.log
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Header: $file_hdr" >> /tmp/script_execution.log

        # Skip header removal for EOD marker files
        if [[ "$FileName" == *_EOD_MARKER_* ]]; then
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] Skipping header removal for EOD marker file: $FileName" >> /tmp/script_execution.log
            continue
        fi

        # Validate before header removal
        pre_lines=$(cat_s3 "$FILE" | wc -l)
        file_size_before=$(cat_s3 "$FILE" | wc -c)
        file_md5_before=$(cat_s3 "$FILE" | md5sum | awk '{print $1}')
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] File $FILE before header removal: lines=$pre_lines, size=${file_size_before} bytes, md5=$file_md5_before" >> /tmp/script_execution.log

        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting header removal: $FILE" >> /tmp/script_execution.log
        sed_s3 -i '/^H/d' "$FILE"
        rc=$?
        if [ $rc -ne 0 ]; then
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: Failed to delete header from $FILE (rc=$rc)" >> /tmp/script_execution.log
            tee_s3 "$LOG_File" "Failed to delete header from $FILE (rc=$rc)"
            exit 1
        fi
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Completed header removal: $FILE" >> /tmp/script_execution.log

    post_lines=$(cat_s3 "$FILE" | wc -l)
    file_size_after=$(cat_s3 "$FILE" | wc -c)
    file_md5_after=$(cat_s3 "$FILE" | md5sum | awk '{print $1}')
    echo "File $FILE after header removal: lines=$post_lines, size=${file_size_after} bytes, md5=$file_md5_after" | tee -a /tmp/script_execution.log
        if [ "$post_lines" -ne $((pre_lines-1)) ]; then
            echo "ERROR: Unexpected line count after header removal for $FILE: before=$pre_lines, after=$post_lines" | tee -a /tmp/script_execution.log
            exit 1
        fi

        base_file=$(basename "$FILE" | sed_s3 's/_validated$//')
        validated_path="${_sdm_incoming_temp_dir}/${base_file}_validated"


    echo "At line 225... Moving file to validated path: $validated_path" | tee -a /tmp/script_execution.log
    echo "At line 226... Base file: $base_file" | tee -a /tmp/script_execution.log

        mv_s3 "$FILE" "$validated_path" || {
            echo "Failed to move $FILE" | tee -a /tmp/script_execution.log
            exit 1
        }
        if ! ls_s3 "$validated_path" > /dev/null 2>&1; then
            echo "ERROR: Validated file not found at $validated_path" | tee -a /tmp/script_execution.log
            exit 1
        fi

        sed_s3 -i "1i $file_hdr" "$validated_path" || {
            echo "Failed to insert header into $validated_path" | tee -a /tmp/script_execution.log
            exit 1
        }
        # Validate after header reinsertion
        final_lines=$(cat_s3 "$validated_path" | wc -l)
        echo "Debug: $validated_path has $final_lines lines after header reinsertion" | tee -a /tmp/script_execution.log
        validated_size=$(cat_s3 "$validated_path" | wc -c)
        validated_md5=$(cat_s3 "$validated_path" | md5sum | awk '{print $1}')
        echo "Validated file $validated_path: lines=$final_lines, size=${validated_size} bytes, md5=$validated_md5" | tee -a /tmp/script_execution.log
        if [ "$final_lines" -ne "$pre_lines" ]; then
            echo "ERROR: Line count mismatch after header reinsertion for $validated_path: expected=$pre_lines, actual=$final_lines" | tee -a /tmp/script_execution.log
            exit 1
        fi
    fi
done

# Create EOD Marker File
eod_marker="${_sdm_incoming_temp_dir}/${_fcountry}_${_appNameUpper}_EOD_MARKER_${_sdm_run_date_formated}_D_1.dat"
if ls_s3 "$eod_marker" > /dev/null 2>&1; then
    rm_s3 "$eod_marker"
fi

echo -e "H\x01EG\x01${_sdm_run_date_formated}\x01${_sdm_run_date_formated}\x011\nD\x01${_sdm_run_date}\nT\x011\x010\x010" | tee -a /tmp/script_execution.log
tee_s3 "$eod_marker" "H\x01EG\x01${_sdm_run_date_formated}\x01${_sdm_run_date_formated}\x011\nD\x01${_sdm_run_date}\nT\x011\x010\x010"

# Copy files to incoming
for FILE in $(ls_s3 "${_sdm_incoming_temp_dir}/*_validated"); do
    dest_file_name=$(basename "$FILE" | sed_s3 's/_validated$//')
    dest_path="$_sdm_incoming_dir/$dest_file_name"
    echo "At 243... Copying file to incoming: $FILE as $dest_path" | tee -a /tmp/script_execution.log
    final_lines=$(cat_s3 "$FILE" | wc -l)
    final_size=$(cat_s3 "$FILE" | wc -c)
    final_md5=$(cat_s3 "$FILE" | md5sum | awk '{print $1}')
    echo "Final file $dest_path: lines=$final_lines, size=${final_size} bytes, md5=$final_md5" | tee -a /tmp/script_execution.log
    if ! cp_s3 "$FILE" "$dest_path"; then
    echo "Failed to copy $FILE to $dest_path" | tee -a /tmp/script_execution.log
        tee_s3 "$LOG_File" "Failed to copy $FILE to $dest_path"
        exit 1
    fi
done


# Cat the known output file for verification
echo "[$(date '+%Y-%m-%d %H:%M:%S')] DEBUG: Entering final file preview block" >> /tmp/script_execution.log
final_file="${_sdm_incoming_dir}/EG_DTP_BALDA_${_sdm_run_date_formated}_D_1.dat"
if ls_s3 "$final_file" > /dev/null 2>&1; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Preview of final file: $final_file" | tee -a /tmp/script_execution.log
    cat_s3 "$final_file" | head -2 | tee -a /tmp/script_execution.log
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: Final file $final_file not found in incoming" | tee -a /tmp/script_execution.log
fi

# Cleanup
cleanup_old_s3 "$_sdm_archive_dir" 10
cleanup_old_s3 "$_sdm_nas_path" 10
cleanup_old_s3 "$LOGDIR" 7

end_time=$(date +%s)
total_time=$((end_time - start_time))
echo "Total time taken by the process: ${total_time} seconds" | tee -a /tmp/script_execution.log
tee_s3 "$LOG_File" "Total time taken by the process: ${total_time} seconds"

exit 0

else
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

_fcountry=`echo "${_countryNameUpper}" | sed_s3 's/GLB1/G1/;s/GLB2/G2/;s/GLB3/G3/;s/GLB4/G4/;s/GLB5/G5/;s/GLB6/G6/'`
_sdm_run_date_formated=$(date +%Y%m%d -d "$3")
_sdm_run_date=$(date +%Y-%m-%d -d "$3")
_sdm_run_date_formated_prev=$(date +%Y%m%d -d "$3 - 7 day")

#PROD PATHS
_sdm_trg_path=s3a://your-bucket/BCBS/SDM/PRD/dtp
_sdm_base_dir=s3a://your-bucket/BCBS/SDM/PRD
_sdm_nas_path=s3a://your-bucket/BCBS/prd/dtp/data/${_countryNameUpper}/nas
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

[ $(ls_s3 /CTRLFW/SOURCG/SCUDEE/SDM/edmp-alerts/cop_log.properties | wc -l) -gt 0 ] && base_log_path=$(cat_s3 /CTRLFW/SOURCG/SCUDEE/SDM/edmp-alerts/cop_log.properties) || base_log_path=s3a://your-bucket/SOURCG/SCUDEE/SDM/PRD/logs
LOG_DIR=$base_log_path/${_appNameLower}
mkdir_s3 $LOG_DIR
LOG_File=$LOG_DIR/${_appNameLower}_${_countryNameLower}_v2_file_copy_$3.log
echo "Script Start Time: `date`"
tee_s3 $LOG_File "Script Start Time: `date`"
echo "File copy processing for country : ${_countryNameUpper}"
tee_s3 $LOG_File "File copy processing for country : ${_countryNameUpper}"

tmp_file_cnt=$(ls_s3 ${_sdm_incoming_temp_dir} | wc -l)
if [ ${tmp_file_cnt} -ge 1 ]
then
  rm_s3 ${_sdm_incoming_temp_dir}/*
fi

if [ $(ls_s3 $LOGDIR | wc -l) -gt 0 ]; then  
        echo "$LOGDIR is available"
else
        echo "$LOGDIRis not available, Creating now"
        mkdir_s3 $LOGDIR
fi

echo "Script Start Time: `date`"
tee_s3 $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log "Script Start Time: `date`"
echo "Script Start Time: `date`"
tee_s3 $LOG_File "Script Start Time: `date`"

echo "BIF_to_sdm_file_copy.sh started"
tee_s3 $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log "BIF_to_sdm_file_copy.sh started"
echo "BIF_to_sdm_file_copy.sh started"
tee_s3 $LOG_File "BIF_to_sdm_file_copy.sh started"
echo "File copy processing for country : ${countryNameUpper}"
tee_s3 $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log "File copy processing for country : ${countryNameUpper}"
echo "File copy processing for country : ${countryNameUpper}"
tee_s3 $LOG_File "File copy processing for country : ${countryNameUpper}"

files_pattern_config_file=${_sdm_trg_path}/${countryNameLower}/appl/${appNameLower}_${countryNameLower}_files_list.txt
if [ $(ls_s3 ${files_pattern_config_file} | wc -l) -gt 0 ]
then
    echo "File pattern config file is present"
    tee_s3 $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log "File pattern config file is present"
    echo "File pattern config file is present"
    tee_s3 $LOG_File "File pattern config file is present"
else
    echo "File pattern config file is not present"
    tee_s3 $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log "File pattern config file is not present"
    echo "File pattern config file is not present"
    tee_s3 $LOG_File "File pattern config file is not present"
        exit 9
fi
tot_file_count=$(cat_s3 ${_sdm_trg_path}/${countryNameLower}/appl/${appNameLower}_${countryNameLower}_files_list.txt | wc -l)
echo "No of files in List file : ${tot_file_count}"
tee_s3 $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log "No of files in List file : ${tot_file_count}"
echo "No of files in List file : ${tot_file_count}"
tee_s3 $LOG_File "No of files in List file : ${tot_file_count}"

config_dir=$(dirname ${files_pattern_config_file})
files_list=${config_dir}/ongoing_copy_file.lst

if [ $(ls_s3 ${files_list} | wc -l) -gt 0 ]; then
   rm_s3 ${files_list}
fi

declare -a FILES_LIST_ARRAY
let i=0

# Validating sed_s3command usage
for FILE in $(cat_s3 ${files_pattern_config_file}); do
    file_with_date=$(echo $FILE | sed_s3 "s/##date##/${_sdm_run_date_formated}/g") || { echo "sed command failed for $FILE"; tee_s3 $LOG_File "sed command failed for $FILE"; exit 1; }
    file_without_ext=$(echo $file_with_date | cut -d "." -f 1)
    echo ${file_without_ext}
    tee_s3 -a ${files_list} "${file_without_ext}"
    FILES_LIST_ARRAY[i]="${file_without_ext}"
    ((++i))
done

echo "*****************************************************"
echo "*****************************************************"
tee_s3 $LOG_File "*****************************************************"
echo ${FILES_LIST_ARRAY[@]}
if [ ${#FILES_LIST_ARRAY[@]} -le 0 ]
then
    echo "Array is empty,Please check the file pattern config file"
    tee_s3 $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log "Array is empty,Please check the file pattern config file"
    echo "Array is empty,Please check the file pattern config file"
    tee_s3 $LOG_File "Array is empty,Please check the file pattern config file"
        exit 9
fi
RES_COUNT=0
j=0
flag=true
for ((i=0; i<${#FILES_LIST_ARRAY[@]}; i++));
do
        if [ ${RES_COUNT} -lt ${tot_file_count} ]; then
                if [ $(ls_s3 ${_sdm_nas_path}/${FILES_LIST_ARRAY[j]}.dat | wc -l) -gt 0 ]; then
                                echo "File exists in  NAS : ${FILES_LIST_ARRAY[j]}.dat"
                                tee_s3 $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log "File exists in  NAS : ${FILES_LIST_ARRAY[j]}.dat"
                                echo "File exists in  NAS : ${FILES_LIST_ARRAY[j]}.dat"
                                tee_s3 $LOG_File "File exists in  NAS : ${FILES_LIST_ARRAY[j]}.dat"
                                (( RES_COUNT++ ))
                                (( j++ ))
                else
                                echo "File ${FILES_LIST_ARRAY[j]}.dat doesn't exist in NAS"
                                tee_s3 $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log "File ${FILES_LIST_ARRAY[j]}.dat doesn't exist in NAS"
                                echo "File ${FILES_LIST_ARRAY[j]}.dat doesn't exist in NAS"
                                tee_s3 $LOG_File "File ${FILES_LIST_ARRAY[j]}.dat doesn't exist in NAS"
                                (( j++ ))
                fi
        fi
done

# Separate echo and tee_s3 commands for clarity
echo "No of files in temp : ${RES_COUNT}"
tee_s3 $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log "No of files in temp : ${RES_COUNT}"

echo "No of files in temp : ${RES_COUNT}"
tee_s3 $LOG_File "No of files in temp : ${RES_COUNT}"

if [ $tot_file_count -eq ${RES_COUNT} ]
then

    echo "File count in NAS matching with SDM file list"
    tee_s3 $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log "File count in NAS matching with SDM file list"
    echo "File count in NAS matching with SDM file list"
    tee_s3 $LOG_File "File count in NAS matching with SDM file list"
    echo "Starting to Move files to incoming_temp" 
    echo "Starting to Move files to incoming_temp"
    tee_s3 $LOG_File "Starting to Move files to incoming_temp"
    cp_s3 ${_sdm_nas_path}/${_fcountry}_${_appNameUpper}_*_${_sdm_run_date_formated}_D_1.dat ${_sdm_incoming_temp_dir}
    echo "Moving of files to  incoming_temp has been completed"
    tee_s3 $LOG_File "Moving of files to  incoming_temp has been completed"

    echo "Unzip  of files to  incoming_temp has been completed"
    tee_s3 $LOG_File "Unzip  of files to  incoming_temp has been completed"

    echo "Script Start Time: `date` "
    tee_s3 $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log "Script Start Time: `date` "
    echo "Script Start Time: `date` "
    tee_s3 $LOG_File "Script Start Time: `date` "

    echo "preprocessing.sh started "
    tee_s3 $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log "preprocessing.sh started "
    echo "preprocessing.sh started "
    tee_s3 $LOG_File "preprocessing.sh started "

    echo "File pre-processing for country : ${_countryNameUpper} "
    tee_s3 $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log "File pre-processing for country : ${_countryNameUpper} "
    echo "File pre-processing for country : ${_countryNameUpper} "
    tee_s3 $LOG_File "File pre-processing for country : ${_countryNameUpper} "
    
    echo "Runnning : s3_Preprocessing_DTP.sh ${_countryNameLower} ${_sdm_run_date_formated}"
    echo "Runnning : s3_Preprocessing_DTP.sh ${_countryNameLower} ${_sdm_run_date_formated}"
    tee_s3 $LOG_File "Runnning : s3_Preprocessing_DTP.sh ${_countryNameLower} ${_sdm_run_date_formated}"
    sh ${_local_ctrlm_path}/s3_Preprocessing_DTP.sh ${_countryNameLower} ${_sdm_run_date_formated}
    
    for FILE in $(ls_s3 ${_sdm_incoming_temp_dir}/${_fcountry}_${_appNameUpper}_*_${_sdm_run_date_formated}_D_1.dat)
    do
        FileName=`basename $FILE`
        file_hdr=$(head_s3 -1 ${_sdm_incoming_temp_dir}/${FileName})
        echo "At line 455... Before calling sed_s3... FileName: ${FileName}"
        echo "Header: ${file_hdr}"
        sed_s3 -i '/^H/d' ${_sdm_incoming_temp_dir}/${FileName}

        base_file=$(basename "${FileName}" | sed_s3 's/_validated$//')
        validated_path="${_sdm_incoming_temp_dir}/${base_file}_validated"
        echo "At line 481... Moving file to validated path: $validated_path"
        echo "At line 482... Base file: $base_file"
        echo "At line 483... Original file: ${FileName}"
        mv_s3 "${_sdm_incoming_temp_dir}/${FileName}" "$validated_path" || {
            echo "Failed to move ${FileName}"
            tee_s3 $LOG_File "Failed to move ${FileName}"
            exit 1
        }
        # Check existence before further processing
        if ! s3_file_exists "$validated_path"; then
            echo "ERROR: Validated file not found at $validated_path"
            tee_s3 $LOG_File "ERROR: Validated file not found at $validated_path"
            base_file=$(basename "$FILE" | sed_s3 's/_validated$//')
            validated_path="${_sdm_incoming_temp_dir}/${base_file}_validated"
            echo "Failed to insert header into $validated_path"
            tee_s3 $LOG_File "Failed to insert header into $validated_path"
            exit 1
        }

        echo "Ravi.. Processing file : ${FileName}"	
        file_ptrn1=`echo ${FileName} | cut -d"_" -f1-4 | sed_s3 -e 's/_${_sdm_run_date_formated}//'`
        file_ptrn=$(echo ${file_ptrn1} | sed_s3 "s/_${_sdm_run_date_formated}//g")
        col_cnt1=$(grep_s3 -i ${file_ptrn} ${_sdm_trg_path}/${_countryNameLower}/configs/col_count.config | cut -d'=' -f2)
        col_cnt=$((col_cnt1 + 1))
        cut_s3 -d"" -f1-${col_cnt} ${_sdm_incoming_temp_dir}/${FileName}_validated  > ${_sdm_incoming_temp_dir}/${FileName}
        sed_s3 -i "1i ${file_hdr}" ${_sdm_incoming_temp_dir}/${FileName}
    done

    echo "Ravi.. Creating EOD Marker File"
    if [ $(ls_s3 ${_sdm_incoming_temp_dir}/${_fcountry}_${_appNameUpper}_EOD_MARKER_${_sdm_run_date_formated}_D_1.dat | wc -l) -gt 0 ]; then
        rm_s3 ${_sdm_incoming_temp_dir}/${_fcountry}_${_appNameUpper}_EOD_MARKER_${_sdm_run_date_formated}_D_1.dat
    fi

    echo "Ravi.. EOD Marker File created"
    for FILE in $(ls_s3 ${_sdm_incoming_temp_dir}/${_fcountry}_${_appNameUpper}_*_${_sdm_run_date_formated}_D_1.dat)
    do
        echo "Ravi.. Copying source files to incoming folder"
        FileName=`basename $FILE`
        cp_s3 ${_sdm_incoming_temp_dir}/${FileName} ${_sdm_incoming_dir}/ || {
            echo "Failed to copy ${FileName}" 2>&1 
            tee_s3 $LOG_File "Failed to copy ${FileName}"; 
            exit 1;
        }
        sleep 3
    done

    echo "Source Files copied to incoming folder"
    tee_s3 $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log "Source Files copied to incoming folder"
    echo "Source Files copied to incoming folder"
    tee_s3 $LOG_File "Source Files copied to incoming folder"

    no_of_files_sdmtemp=$(ls_s3 ${_sdm_incoming_temp_dir}/${_fcountry}_${_appNameUpper}_*_${_sdm_run_date_formated}_D_1.dat | wc -l)

    if [ $tot_file_count -eq $no_of_files_sdmtemp ]
    then
        echo -e "HEG${_sdm_run_date_formated}${_sdm_run_date_formated}1\nD${_sdm_run_date}\nT100" 
        tee_s3 ${_sdm_incoming_temp_dir}/${_fcountry}_${_appNameUpper}_EOD_MARKER_${_sdm_run_date_formated}_D_1.dat "HEG${_sdm_run_date_formated}${_sdm_run_date_formated}1\nD${_sdm_run_date}\nT100"
    echo "EOD Marker File created"
    tee_s3 $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log "EOD Marker File created"
    echo "EOD Marker File created"
    tee_s3 $LOG_File "EOD Marker File created"
        cp_s3 ${_sdm_incoming_temp_dir}/${_fcountry}_${_appNameUpper}_EOD_MARKER_${_sdm_run_date_formated}_D_1.dat ${_sdm_incoming_dir}/
    echo "Script End Time: `date` "
    tee_s3 $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log "Script End Time: `date` "
    echo "Script End Time: `date` "
    tee_s3 $LOG_File "Script End Time: `date` "
        rm_s3 ${_sdm_incoming_temp_dir}/*_validated
        cleanup_old_s3 ${_sdm_archive_dir} 10
        cleanup_old_s3 ${_sdm_nas_path} 10
        cleanup_old_s3 ${LOGDIR} 7
        exit 0
    else
        end_time=$(date +%s)
        total_time=$((end_time - start_time))
    echo "Total time taken by the process: ${total_time} seconds"
    tee_s3 $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log "Total time taken by the process: ${total_time} seconds"
        exit 9
    fi

else
    echo "file count doesnt matches between NAS and SDM list file"
    tee_s3 $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log "file count doesnt matches between NAS and SDM list file"
    end_time=$(date +%s)
    total_time=$((end_time - start_time))
    echo "Total time taken by the process: ${total_time} seconds" 
    tee_s3 $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log "Total time taken by the process: ${total_time} seconds"
    exit 9
fi
fi

### Sending mail to source in case files not received after cut off time ###

if [ ${missedfilestatus} -eq 1 ];then
if [ $(ls_s3 $_ctrlm_path/${countryNameLower}_counterFile_${_sdm_run_date_formated}.txt | wc -l) -gt 0 ];then
vRunCounter=$(cat_s3 $_ctrlm_path/${countryNameLower}_counterFile_${_sdm_run_date_formated}.txt)
vRunCounter=`expr ${vRunCounter} + 1`
echo ${vRunCounter}
tee_s3 $_ctrlm_path/${countryNameLower}_counterFile_${_sdm_run_date_formated}.txt ${vRunCounter}

if [ ${vRunCounter} -ge 7 ];then
# mail command cannot be run from pod, consider using an API call or notification service
echo "File has not received. sending mail to source team."
tee_s3 $LOGDIR/dtp_sdm_file_preprocessing_${countryNameLower}_$_sdm_run_date.log "File has not received. sending mail to source team."
echo "File has not received. sending mail to source team."
tee_s3 $LOG_File "File has not received. sending mail to source team."
exit 2
fi
echo "OLA Cut-off time has not reached. hence ingoring and waiting next cycle"
echo "OLA Cut-off time has not reached. hence ingoring and waiting next cycle"
tee_s3 $LOG_File "OLA Cut-off time has not reached. hence ingoring and waiting next cycle"
exit 9

else
vRunCounter=1
echo $vRunCounter
tee_s3 $_ctrlm_path/${countryNameLower}_counterFile_${_sdm_run_date_formated}.txt $vRunCounter
echo "OLA Cut-off time has not reached. hence ingoring and waiting next cycle"
exit 9
fi
fi

exit 0

else
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

_fcountry=`echo "${_countryNameUpper}" | sed_s3 's/GLB1/G1/;s/GLB2/G2/;s/GLB3/G3/;s/GLB4/G4/;s/GLB5/G5/;s/GLB6/G6/'`
_sdm_run_date_formated=$(date +%Y%m%d -d "$3")
_sdm_run_date=$(date +%Y-%m-%d -d "$3")
_sdm_run_date_formated_prev=$(date +%Y%m%d -d "$3 - 7 day")

#PROD PATHS
_sdm_trg_path=s3a://your-bucket/BCBS/SDM/PRD/dtp
_sdm_base_dir=s3a://your-bucket/BCBS/SDM/PRD
_sdm_nas_path=s3a://your-bucket/BCBS/prd/dtp/data/${_countryNameUpper}/nas
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

[ $(ls_s3 /CTRLFW/SOURCG/SCUDEE/SDM/edmp-alerts/cop_log.properties | wc -l) -gt 0 ] && base_log_path=$(cat_s3 /CTRLFW/SOURCG/SCUDEE/SDM/edmp-alerts/cop_log.properties) || base_log_path=s3a://your-bucket/SOURCG/SCUDEE/SDM/PRD/logs
LOG_DIR=$base_log_path/${_appNameLower}
mkdir_s3 $LOG_DIR
LOG_File=$LOG_DIR/${_appNameLower}_${_countryNameLower}_v2_file_copy_$3.log
tee_s3 "$LOG_File" "Script Start Time: $(date)"
tee_s3 "$LOG_File" "File copy processing for country : ${_countryNameUpper}"

tmp_file_cnt=$(ls_s3 ${_sdm_incoming_temp_dir} | wc -l)
if [ ${tmp_file_cnt} -ge 1 ]
then
  rm_s3 ${_sdm_incoming_temp_dir}/*
fi

if [ $(ls_s3 $LOGDIR | wc -l) -gt 0 ]; then  
        echo "$LOGDIR is available"
else
        echo "$LOGDIRis not available, Creating now"
        mkdir_s3 $LOGDIR
fi

echo "Script Start Time: `date`"
tee_s3 $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log "Script Start Time: `date`"
echo "Script Start Time: `date`"
tee_s3 $LOG_File "Script Start Time: `date`"

echo "BIF_to_sdm_file_copy.sh started"
tee_s3 $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log "BIF_to_sdm_file_copy.sh started"
echo "BIF_to_sdm_file_copy.sh started"
tee_s3 $LOG_File "BIF_to_sdm_file_copy.sh started"
echo "File copy processing for country : ${countryNameUpper}"
tee_s3 $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log "File copy processing for country : ${countryNameUpper}"
echo "File copy processing for country : ${countryNameUpper}"
tee_s3 $LOG_File "File copy processing for country : ${countryNameUpper}"

files_pattern_config_file=${_sdm_trg_path}/${countryNameLower}/appl/${appNameLower}_${countryNameLower}_files_list.txt
if [ $(ls_s3 ${files_pattern_config_file} | wc -l) -gt 0 ]
then
        echo "File pattern config file is present"
        tee_s3 $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log "File pattern config file is present"
        echo "File pattern config file is present"
        tee_s3 $LOG_File "File pattern config file is present"
else
        echo "File pattern config file is not present"
        tee_s3 $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log "File pattern config file is not present"
        echo "File pattern config file is not present"
        tee_s3 $LOG_File "File pattern config file is not present"
        exit 9
fi
tot_file_count=$(cat_s3 ${_sdm_trg_path}/${countryNameLower}/appl/${appNameLower}_${countryNameLower}_files_list.txt | wc -l)
echo "No of files in List file : ${tot_file_count}"
tee_s3 $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log "No of files in List file : ${tot_file_count}"
echo "No of files in List file : ${tot_file_count}"
tee_s3 $LOG_File "No of files in List file : ${tot_file_count}"

config_dir=$(dirname ${files_pattern_config_file})
files_list=${config_dir}/ongoing_copy_file.lst

if [ $(ls_s3 ${files_list} | wc -l) -gt 0 ]; then
   rm_s3 ${files_list}
fi

declare -a FILES_LIST_ARRAY
let i=0

# Validating sed_s3command usage
for FILE in $(cat_s3 ${files_pattern_config_file}); do
    file_with_date=$(echo $FILE | sed_s3 "s/##date##/${_sdm_run_date_formated}/g") || 
    { echo "sed command failed for $FILE" 2>&1 
    tee_s3 $LOG_File "sed command failed for $FILE"; exit 1; }
    file_without_ext=$(echo $file_with_date | cut -d "." -f 1)
    echo ${file_without_ext}
    tee_s3 -a ${files_list} "${file_without_ext}"
    FILES_LIST_ARRAY[i]="${file_without_ext}"
    ((++i))
done

echo "*****************************************************"
echo "*****************************************************"
tee_s3 $LOG_File "*****************************************************"
echo ${FILES_LIST_ARRAY[@]}
if [ ${#FILES_LIST_ARRAY[@]} -le 0 ]
then
        echo "Array is empty,Please check the file pattern config file"
        tee_s3 $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log "Array is empty,Please check the file pattern config file"
        echo "Array is empty,Please check the file pattern config file"
        tee_s3 $LOG_File "Array is empty,Please check the file pattern config file"
        exit 9
fi
RES_COUNT=0
j=0
flag=true
for ((i=0; i<${#FILES_LIST_ARRAY[@]}; i++));
do
        if [ ${RES_COUNT} -lt ${tot_file_count} ]; then
                if [ $(ls_s3 ${_sdm_nas_path}/${FILES_LIST_ARRAY[j]}.dat | wc -l) -gt 0 ]; then
                                echo "File exists in  NAS : ${FILES_LIST_ARRAY[j]}.dat"
                                tee_s3 $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log "File exists in  NAS : ${FILES_LIST_ARRAY[j]}.dat"
                                echo "File exists in  NAS : ${FILES_LIST_ARRAY[j]}.dat"
                                tee_s3 $LOG_File "File exists in  NAS : ${FILES_LIST_ARRAY[j]}.dat"
                                (( RES_COUNT++ ))
                                (( j++ ))
                else
                                echo "File ${FILES_LIST_ARRAY[j]}.dat doesn't exist in NAS"
                                tee_s3 $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log "File ${FILES_LIST_ARRAY[j]}.dat doesn't exist in NAS"
                                echo "File ${FILES_LIST_ARRAY[j]}.dat doesn't exist in NAS"
                                tee_s3 $LOG_File "File ${FILES_LIST_ARRAY[j]}.dat doesn't exist in NAS"
                                (( j++ ))
                fi
        fi
done

# Separate echo and tee_s3 commands for clarity
echo "No of files in temp : ${RES_COUNT}"
tee_s3 $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log "No of files in temp : ${RES_COUNT}"

echo "No of files in temp : ${RES_COUNT}"
tee_s3 $LOG_File "No of files in temp : ${RES_COUNT}"

if [ $tot_file_count -eq ${RES_COUNT} ]
then

    echo "File count in NAS matching with SDM file list"
    tee_s3 "$LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log" "File count in NAS matching with SDM file list"
    echo "File count in NAS matching with SDM file list"
    tee_s3 "$LOG_File" "File count in NAS matching with SDM file list"
    echo "Starting to Move files to incoming_temp"
    tee_s3 "$LOG_File" "Starting to Move files to incoming_temp"
    cp_s3 ${_sdm_nas_path}/${_fcountry}_${_appNameUpper}_*_${_sdm_run_date_formated}_D_1.dat ${_sdm_incoming_temp_dir}
    echo "Moving of files to  incoming_temp has been completed"
    tee_s3 "$LOG_File" "Moving of files to  incoming_temp has been completed"

    echo "Unzip  of files to  incoming_temp has been completed"
    tee_s3 "$LOG_File" "Unzip  of files to  incoming_temp has been completed"

    echo "Script Start Time: $(date)"
    tee_s3 "$LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log" "Script Start Time: $(date)"
    echo "Script Start Time: $(date)"
    tee_s3 "$LOG_File" "Script Start Time: $(date)"

    echo "preprocessing.sh started "
    tee_s3 "$LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log" "preprocessing.sh started "
    echo "preprocessing.sh started "
    tee_s3 "$LOG_File" "preprocessing.sh started "

    echo "File pre-processing for country : ${_countryNameUpper} "
    tee_s3 "$LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log" "File pre-processing for country : ${_countryNameUpper} "
    echo "File pre-processing for country : ${_countryNameUpper} "
    tee_s3 "$LOG_File" "File pre-processing for country : ${_countryNameUpper} "
    
    echo "Runnning : s3_Preprocessing_DTP.sh ${_countryNameLower} ${_sdm_run_date_formated}"
    tee_s3 "$LOG_File" "Runnning : s3_Preprocessing_DTP.sh ${_countryNameLower} ${_sdm_run_date_formated}"
    sh ${_local_ctrlm_path}/s3_Preprocessing_DTP.sh ${_countryNameLower} ${_sdm_run_date_formated}
    
    for FILE in $(ls_s3 ${_sdm_incoming_temp_dir}/${_fcountry}_${_appNameUpper}_*_${_sdm_run_date_formated}_D_1.dat)
    do
        FileName=`basename $FILE`
        file_hdr=$(head_s3 -1 ${_sdm_incoming_temp_dir}/${FileName})
        sed_s3 -i '/^H/d' ${_sdm_incoming_temp_dir}/${FileName}
        
        base_file=$(basename "${FileName}" | sed_s3 's/_validated$//')
        validated_path="${_sdm_incoming_temp_dir}/${base_file}_validated"

        echo "At line 809... base file: $base_file"
        echo "At line 810... Moving file to validated path: $validated_path"
        echo "At line 811... Original file: ${FileName}"

        mv_s3 "${_sdm_incoming_temp_dir}/${FileName}" "$validated_path" || {
            echo "Failed to move ${FileName}" 2>&1
            tee_s3 $LOG_File "Failed to move ${FileName}"
            exit 1;
        }
        echo "Ravi.. Processing file : ${FileName}"	
        file_ptrn1=`echo ${FileName} | cut -d"_" -f1-4 | sed_s3 -e 's/_${_sdm_run_date_formated}//'`
        file_ptrn=$(echo ${file_ptrn1} | sed_s3 "s/_${_sdm_run_date_formated}//g")
        col_cnt1=$(grep_s3 -i ${file_ptrn} ${_sdm_trg_path}/${_countryNameLower}/configs/col_count.config | cut -d'=' -f2)
        col_cnt=$((col_cnt1 + 1))
        cut_s3 -d"" -f1-${col_cnt} ${_sdm_incoming_temp_dir}/${FileName}_validated  > ${_sdm_incoming_temp_dir}/${FileName}
        sed_s3 -i "1i ${file_hdr}" ${_sdm_incoming_temp_dir}/${FileName}
    done

    echo "Ravi.. Creating EOD Marker File"
    if [ $(ls_s3 ${_sdm_incoming_temp_dir}/${_fcountry}_${_appNameUpper}_EOD_MARKER_${_sdm_run_date_formated}_D_1.dat | wc -l) -gt 0 ]; then
        rm_s3 ${_sdm_incoming_temp_dir}/${_fcountry}_${_appNameUpper}_EOD_MARKER_${_sdm_run_date_formated}_D_1.dat
    fi

    echo "Ravi.. EOD Marker File created"
    for FILE in $(ls_s3 ${_sdm_incoming_temp_dir}/${_fcountry}_${_appNameUpper}_*_${_sdm_run_date_formated}_D_1.dat)
    do
        echo "Ravi.. Copying source files to incoming folder"
        FileName=`basename $FILE`
        cp_s3 ${_sdm_incoming_temp_dir}/${FileName} ${_sdm_incoming_dir}/ || {
            echo "Failed to copy ${FileName}" 2>&1 
            tee_s3 $LOG_File "Failed to copy ${FileName}" 
            exit 1;
        }
        sleep 3
    done

    echo "Source Files copied to incoming folder"
    tee_s3 $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log "Source Files copied to incoming folder"
    echo "Source Files copied to incoming folder"
    tee_s3 $LOG_File "Source Files copied to incoming folder"

    no_of_files_sdmtemp=$(ls_s3 ${_sdm_incoming_temp_dir}/${_fcountry}_${_appNameUpper}_*_${_sdm_run_date_formated}_D_1.dat | wc -l)

    if [ $tot_file_count -eq $no_of_files_sdmtemp ]
    then
        echo -e "HEG${_sdm_run_date_formated}${_sdm_run_date_formated}1\nD${_sdm_run_date}\nT100" 
        tee_s3 ${_sdm_incoming_temp_dir}/${_fcountry}_${_appNameUpper}_EOD_MARKER_${_sdm_run_date_formated}_D_1.dat "HEG${_sdm_run_date_formated}${_sdm_run_date_formated}1\nD${_sdm_run_date}\nT100"
    echo "EOD Marker File created"
    tee_s3 $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log "EOD Marker File created"
    echo "EOD Marker File created"
    tee_s3 $LOG_File "EOD Marker File created"
        cp_s3 ${_sdm_incoming_temp_dir}/${_fcountry}_${_appNameUpper}_EOD_MARKER_${_sdm_run_date_formated}_D_1.dat ${_sdm_incoming_dir}/
    echo "Script End Time: `date` "
    tee_s3 $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log "Script End Time: `date` "
    echo "Script End Time: `date` "
    tee_s3 $LOG_File "Script End Time: `date` "
        rm_s3 ${_sdm_incoming_temp_dir}/*_validated
        cleanup_old_s3 ${_sdm_archive_dir} 10
        cleanup_old_s3 ${_sdm_nas_path} 10
        cleanup_old_s3 ${LOGDIR} 7
        exit 0
    else
        end_time=$(date +%s)
        total_time=$((end_time - start_time))
    tee_s3 $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log "Total time taken by the process: ${total_time} seconds"
        exit 9
    fi

else
    echo "file count doesnt matches between NAS and SDM list file"
    tee_s3 $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log "file count doesnt matches between NAS and SDM list file"
    end_time=$(date +%s)
    total_time=$((end_time - start_time))
    echo "Total time taken by the process: ${total_time} seconds" 
    tee_s3 $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log "Total time taken by the process: ${total_time} seconds"
    exit 9
fi
fi

# Separate echo and tee_s3 commands for clarity
echo "File pattern config file is not present"
tee_s3 "$LOG_File" "File pattern config file is not present"

echo "File list is empty. Please check the file pattern config file."
tee_s3 "$LOG_File" "File list is empty. Please check the file pattern config file."

echo "sed command failed for $FILE"
tee_s3 "$LOG_File" "sed command failed for $FILE"

echo "File not found in NAS: $FILE"
tee_s3 "$LOG_File" "File not found in NAS: $FILE"

echo "File count mismatch between NAS and SDM list file"
tee_s3 "$LOG_File" "File count mismatch between NAS and SDM list file"

echo "Failed to copy $FILE to $_sdm_incoming_temp_dir"
tee_s3 "$LOG_File" "Failed to copy $FILE to $_sdm_incoming_temp_dir"

echo "Failed to move $FILE"
tee_s3 "$LOG_File" "Failed to move $FILE"

echo -e "H\x01EG\x01${_sdm_run_date_formated}\x01${_sdm_run_date_formated}\x011\nD\x01${_sdm_run_date}\nT\x011\x010\x010"
tee_s3 "$eod_marker" -e "H\x01EG\x01${_sdm_run_date_formated}\x01${_sdm_run_date_formated}\x011\nD\x01${_sdm_run_date}\nT\x011\x010\x010"

echo "Failed to copy $FILE to $_sdm_incoming_dir"
tee_s3 "$LOG_File" "Failed to copy $FILE to $_sdm_incoming_dir"

echo "Total time taken by the process: ${total_time} seconds"
tee_s3 "$LOG_File" "Total time taken by the process: ${total_time} seconds"

echo "Script Start Time: `date`"
tee_s3 $LOG_File "Script Start Time: `date`"

echo "File copy processing for country : ${_countryNameUpper}"
tee_s3 $LOG_File "File copy processing for country : ${_countryNameUpper}"

echo "Script Start Time: `date`"
tee_s3 $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log "Script Start Time: `date`"

echo "Script Start Time: `date`"
tee_s3 $LOG_File "Script Start Time: `date`"

echo "BIF_to_sdm_file_copy.sh started"
tee_s3 $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log "BIF_to_sdm_file_copy.sh started"

echo "BIF_to_sdm_file_copy.sh started"
tee_s3 $LOG_File "BIF_to_sdm_file_copy.sh started"

echo "File copy processing for country : ${countryNameUpper}"
tee_s3 $LOGDIR/dtp_sdm_file_preprocessing_${_countryNameLower}_$_sdm_run_date.log "File copy processing for country : ${countryNameUpper}"

echo "File copy processing for country : ${countryNameUpper}"
tee_s3 $LOG_File "File copy processing for country : ${countryNameUpper}"