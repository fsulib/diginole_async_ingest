import os
import time

def ais_log(package, message):
  timestamp = int(time.time())
  apache_name = os.getenv('APACHE_CONTAINER_NAME')
  os.system("docker exec apache bash -c "cd /var/www/html; drush sql-query \"insert into diginole_ais_log (time, package, message) values (`date +%s`, 'test.zip', 'This is a test message.');\""
  return hey 
