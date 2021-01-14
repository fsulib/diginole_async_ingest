#!/bin/bash

PACKAGE_KEY=$1
SUBMITTED_ETAG=$2
PACKAGE_FILE=$(basename $PACKAGE_KEY)
PACKAGE_ID=$(basename $PACKAGE_KEY .zip)
PACKAGE_LOG=/diginole_async_ingest/logs/$PACKAGE_ID.log
S3_BUCKET='async-ingest.isle.lib.fsu.edu'

echo "$(date): download-s3-package.sh triggered for file $PACKAGE_FILE with eTag $SUBMITTED_ETAG..." >> $PACKAGE_LOG

# Sleep for 15 minutes to give submitter time to delete file if its incorrect. 
echo "$(date): Sleeping for 15 minutes..." >> $PACKAGE_LOG
sleep 900
echo "$(date): Sleep complete." >> $PACKAGE_LOG

# Check to see if the object with the submitted key is still available and has the same ETag now as it did when submitted in order to
# make sure that if someone uploads a file, then deletes it and submits a new one with the same name, we don't treat them as the same file.
echo "$(date): Checking to see if $S3_BUCKET/$PACKAGE_KEY still exists..." >> $PACKAGE_LOG
aws s3 ls s3://$S3_BUCKET/$PACKAGE_KEY >> $PACKAGE_LOG
if [ $? = 0 ]
then
  echo "$(date): Success, $S3_BUCKET/$PACKAGE_KEY still exists!" >> $PACKAGE_LOG
  echo "$(date): Retrieving current eTag for $PACKAGE_FILE..." >> $PACKAGE_LOG
  CURRENT_ETAG=$(aws s3api head-object --bucket $S3_BUCKET --key $PACKAGE_KEY --query ETag | tr -d '"\')
  echo "$(date): Current eTag for $PACKAGE_FILE is $CURRENT_ETAG." >> $PACKAGE_LOG
  echo "$(date): Comparing eTags for $PACKAGE_FILE: Original: $SUBMITTED_ETAG. Current: $CURRENT_ETAG." >> $PACKAGE_LOG
  if [ $SUBMITTED_ETAG = $CURRENT_ETAG ]
  then
    echo "$(date): $PACKAGE_FILE eTags match!" >> $PACKAGE_LOG
    PROCEED_WITH_DOWNLOAD=true
  else
    echo "$(date): $PACKAGE_FILE eTags do not match, the package that triggered this script is not the currently available package. Exiting." >> $PACKAGE_LOG
    PROCEED_WITH_DOWNLOAD=false
  fi
else
  echo "$(date): Error, $S3_BUCKET/$PACKAGE_KEY does not still exist." >> $PACKAGE_LOG
fi

if [ $PROCEED_WITH_DOWNLOAD = true ]
then
  echo "$(date): Proceeding to download $S3_BUCKET/$PACKAGE_KEY..." >> $PACKAGE_LOG 
  aws s3 cp s3://$S3_BUCKET/$PACKAGE_KEY /diginole_async_ingest/packages/$PACKAGE_FILE >> $PACKAGE_LOG
  if [ $? = 0 ]
  then
    echo "$(date): $S3_BUCKET/$PACKAGE_KEY successfully downloaded to /diginole_async_ingest/packages/$PACKAGE_FILE." >> $PACKAGE_LOG 
    echo "$(date): Moving $S3_BUCKET/$PACKAGE_KEY to $S3_BUCKET/processing/$PACKAGE_FILE..." >> $PACKAGE_LOG 
    aws s3 mv s3://$S3_BUCKET/$PACKAGE_KEY s3://$S3_BUCKET/processing/$PACKAGE_FILE >> $PACKAGE_LOG
    if [ $? = 0 ]
    then
      echo "$(date): $S3_BUCKET/$PACKAGE_KEY successfully moved to $S3_BUCKET/processing/$PACKAGE_FILE." >> $PACKAGE_LOG 
    else
      echo "$(date): Error moving $S3_BUCKET/$PACKAGE_KEY to $S3_BUCKET/processing/$PACKAGE_FILE." >> $PACKAGE_LOG 
    fi
  else
    echo "$(date): Error downloading $S3_BUCKET/$PACKAGE_KEY, exiting." >> $PACKAGE_LOG 
  fi
fi

echo "$(date): download-s3-package.sh finished." >> $PACKAGE_LOG 
