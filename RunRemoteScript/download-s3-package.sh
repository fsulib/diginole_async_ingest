#!/bin/bash

PACKAGE_KEY=$1
PACKAGE_FILE=$(basename $PACKAGE_KEY)
PACKAGE_ID=$(basename $PACKAGE_KEY .zip)
PACKAGE_LOG=/diginole_async_ingest/logs/$PACKAGE_ID.log
S3_BUCKET='async-ingest.isle.lib.fsu.edu'

echo "$(date): download-s3-package.sh starting..." >> $PACKAGE_LOG 

aws s3 ls s3://$S3_BUCKET/$PACKAGE_KEY >> $PACKAGE_LOG

if [ $? = 0 ]
then

  echo "$(date): $S3_BUCKET/$PACKAGE_KEY determined to still be available, downloading..." >> $PACKAGE_LOG 
  aws s3 cp s3://$S3_BUCKET/$PACKAGE_KEY /diginole_async_ingest/packages/$PACKAGE_FILE >> $PACKAGE_LOG 

  if [ $? = 0 ]
  then

    echo "$(date): $S3_BUCKET/$PACKAGE_KEY successfully downloaded to /diginole_async_ingest/packages/$PACKAGE_FILE." >> $PACKAGE_LOG 

    echo "$(date): Tagging $S3_BUCKET/$PACKAGE_KEY with processing metadata..." >> $PACKAGE_LOG 
    aws s3api put-object-tagging \
      --bucket $S3_BUCKET \
      --key $PACKAGE_KEY \
      --tagging "{\"TagSet\": [{\"Key\": \"status\", \"Value\": \"downloaded\" },{ \"Key\": \"downloaded\", \"Value\": \"$(date)\"}]}" \
      >> $PACKAGE_LOG

    if [ $? = 0 ]
    then
      echo "$(date): $S3_BUCKET/$PACKAGE_KEY successfully tagged with processing metadata." >> $PACKAGE_LOG 
    else
      echo "$(date): Error tagging $S3_BUCKET/$PACKAGE_KEY with processing metadata." >> $PACKAGE_LOG 
    fi

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
else
  echo "$(date): Error, $S3_BUCKET/$PACKAGE_KEY determined to be unavailable." >> $PACKAGE_LOG 
fi

echo "$(date): download-s3-package.sh finished." >> $PACKAGE_LOG 
