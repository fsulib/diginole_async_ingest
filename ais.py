# Imports import collections
import configparser
import datetime
import glob
import json
import os
import time
import zipfile


# Variables
s3_bucket = os.getenv('DIGINOLE_AIS_S3BUCKET')
s3_path = "{0}/diginole/ais".format(s3_bucket)
package_path = '/diginole_async_ingest/packages'


# Stand Alone Functions
def log(message):
  os.system("./log.sh '{0}'".format(message))
  print(message)

def move_new_s3_package(package, destination):
  os.system('aws s3 mv s3://{0}/new/{1} s3://{0}/{2}/{1}'.format(s3_path, package, destination))

def delete_downloaded_package(package):
  os.remove("{0}/{1}".format(package_path, package))

def check_downloaded_packages():
  downloaded_packages = glob.glob("{0}/*.zip".format(package_path))
  if len(downloaded_packages) > 0:
    return downloaded_packages
  else:
    return False


# Compound Functions
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
        package_extension = package_name.rpartition('.')[2]
        if package_extension != 'zip':
          move_new_s3_package(package_name, 'error')
          log("Package {0}/new/{1} detected, but is not a zip file. Package moved to {0}/error/{1}.".format(s3_path, package_name))
        else:
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
  return oldest_new_package_name

def validate_package(package_name):
  package_errors = []
  package = zipfile.ZipFile("{0}/{1}".format(package_path, package_name), 'r')
  package_contents = package.namelist()

  # Validate manifest.ini file
  if 'manifest.ini' not in package_contents:
    package_errors.append('Missing manifest.ini file')
  else:
    manifest = configparser.ConfigParser()
    manifest.read_string(package.read('manifest.ini').decode('utf-8'))
    if 'package' not in manifest.sections():
      package_errors.append('manifest.ini missing [package] section')
    else:
      package_metadata = {}
      package_metadata['submitter_email'] = manifest['package']['submitter_email']
      package_metadata['content_model'] = manifest['package']['content_model']
      package_metadata['parent_collection'] = manifest['package']['parent_collection']
      print(package_metadata)
  
  
  # Validate package filenames + IID
  # Validate package contents

  if len(package_errors) > 0:
    return package_errors
  else:
    return True


# Main function
def run():
  if check_new_packages():
    package_name = download_oldest_new_package()
    
