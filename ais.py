import collections
import datetime
import glob
import json
import os
import time

s3_bucket = os.getenv('DIGINOLE_AIS_S3BUCKET')
s3_path = "{0}/diginole/ais".format(s3_bucket)
package_path = '/diginole_async_ingest/packages'

def log(message):
  os.system("./log.sh '{0}'".format(message))
  print(message)

def list_new_packages():
  packages = {}
  output = os.popen('aws s3 ls s3://{0}/new --recursive'.format(s3_path)).readlines()
  for line in output:
    spline = line.split()
    if spline[3] != 'diginole/ais/new/':
      package_name = spline[3].split('/')[-1]
      package_mod_date = spline[0].split('-')
      package_mod_time = spline[1].split(':') 
      pdt = datetime.datetime(
        int(package_mod_date[0]),
        int(package_mod_date[1]),
        int(package_mod_date[2]),
        int(package_mod_time[0]),
        int(package_mod_time[1]),
        int(package_mod_time[2]),
      )
      package_mod_timestamp = int(time.mktime(pdt.timetuple()))
      curtime = int(time.time())
      package_age = curtime - package_mod_timestamp
      if package_age > 900:
        package_key = str(package_mod_timestamp)
        packages[package_key] = package_name
  sorted_packages = collections.OrderedDict(sorted(packages.items()))
  sorted_packages_list = list(sorted_packages.items())
  return sorted_packages_list

def check_new_packages():
  packages = list_new_packages()
  if len(packages) > 0:
    return True
  else:
    return False

def download_oldest_new_package():
  packages = list_new_packages()
  oldest_new_package = packages[0]
  oldest_new_package_name = oldest_new_package[1]
  os.system('aws s3 cp s3://{0}/new/{1} {2}/{1}'.format(s3_path, oldest_new_package_name, package_path))
  log("{0} detected and downloaded to {1}/{0}.".format(oldest_new_package_name, package_path))

def move_new_s3_package(package, destination):
  os.system('aws s3 mv s3://{0}/new/{1} s3://{0}/{2}/{1}'.format(s3_path, package, destination))

def check_downloaded_packages():
  downloaded_packages = glob.glob("{0}/*.zip".format(package_path))
  if len(downloaded_packages) > 0:
    return downloaded_packages
  else:
    return False

def delete_downloaded_package(package):
  os.remove("{0}/{1}".format(package_path, package))

def run():
  if check_new_packages():
    download_oldest_new_package()
    
