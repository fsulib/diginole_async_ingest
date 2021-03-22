#!/bin/bash

source /etc/environment

docker exec $APACHE_CONTAINER_NAME bash -c "cd /var/www/html; drush sql-query \"insert into diginole_ais_log (time, message) values (`date +%s`, '$1');\""
