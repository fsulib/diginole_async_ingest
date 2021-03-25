# Imports import collections
import collections
import configparser
import datetime
import glob
import json
import os
import time
import xml.etree.ElementTree
import zipfile


# Variables
pidfile_path = '/tmp/ais.pid'
apache_name = os.getenv('APACHE_CONTAINER_NAME')
s3_bucket = os.getenv('DIGINOLE_AIS_S3BUCKET')
s3_path = "{0}/diginole/ais".format(s3_bucket)
s3_wait = 0 # Set to 900 for a 15 minute wait
package_path = '/diginole_async_ingest/packages'
cmodels = [
  'islandora:sp_pdf',
  'ir:thesisCModel',
  'ir:citationCModel',
  'islandora:sp_basic_image',
  'islandora:sp_large_image_cmodel',
  'islandora:sp-audioCModel',
  'islandora:sp_videoCModel',
  #'islandora:collectionCModel',
  #'islandora:binaryObjectCModel',
  #'islandora:compoundCModel',
  #'islandora:bookCModel',
  #'islandora:newspaperCModel',
  #'islandora:newspaperIssueCModel'
]


# Independent Functions
def write_pidfile():
  pid = os.getpid()
  print(pid, file=open(pidfile_path, 'w'))
  
def delete_pidfile():
  os.unlink(pidfile_path)
  
def check_pidfile():
  if os.path.isfile(pidfile_path):
    return True
  else:
    return False

def get_current_time():
  return int(time.time())

def log(message):
  current_time = get_current_time()
  logcmd = 'docker exec {0} bash -c "drush --root=/var/www/html sql-query \\"insert into diginole_ais_log (time, message) values ({1}, \'{2}\');\\""'.format(apache_name, current_time, message)
  os.system(logcmd)
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

def get_file_extension(filename):
  return filename.rpartition('.')[2]

def get_file_basename(filename):
  return filename.rpartition('.')[0]

def get_drupaluid_from_email(email):
  cmd = "docker exec {0} bash -c 'drush --root=/var/www/html user:information {1} --format=csv --fields=uid 2>&1'".format(apache_name, email)
  output = os.popen(cmd).readlines()
  if len(output) > 1:
    return 1
  else:
    return int(output[0])

def get_iid_exempt_cmodels():
  cmdstr = "docker exec {0} bash -c 'drush --root=/var/www/html vget diginole_purlz_exempt_cmodels'".format(apache_name)
  output = os.popen(cmdstr).readlines()[0].lstrip('diginole_purlz_exempt_cmodels: ').lstrip("'").rstrip().rstrip("'")
  return output.split(', ')


# Dependent Functions
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
      current_time = get_current_time() 
      package_age = current_time - package_mod_timestamp
      if package_age > s3_wait:
        package_extension = get_file_extension(package_name) 
        if package_extension != 'zip':
          move_new_s3_package(package_name, 'error')
          log("New package {0}/new/{1} detected, but is not a zip file. Package moved to {0}/error/{1}.".format(s3_path, package_name))
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
  log("New package {0}/new/{1} detected and downloaded to {2}/{1}.".format(s3_path, oldest_new_package_name, package_path))
  return oldest_new_package_name

def validate_package(package_name):
  package_metadata = {'filename': package_name}
  package_errors = []
  package = zipfile.ZipFile("{0}/{1}".format(package_path, package_name), 'r')
  package_contents = package.namelist()

  if 'manifest.ini' not in package_contents:
    package_errors.append('Missing manifest.ini file')
  else:
    manifest = configparser.ConfigParser()
    manifest.read_string(package.read('manifest.ini').decode('utf-8'))
    if 'package' not in manifest.sections():
      package_errors.append('manifest.ini missing [package] section')
    else:
      package_metadata['submitter_email'] = manifest['package']['submitter_email'] if 'submitter_email' in manifest['package'].keys() else package_errors.append('manifest.ini missing submitter_email')
      package_metadata['parent_collection'] = manifest['package']['parent_collection'] if 'parent_collection' in manifest['package'].keys() else package_errors.append('manifest.ini missing parent_collection')
      package_metadata['content_model'] = manifest['package']['content_model'] if 'content_model' in manifest['package'].keys() else package_errors.append('manifest.ini missing content_model')
      if package_metadata['content_model'] not in cmodels:
        package_errors.append("'{0}' is not a valid content model".format(package_metadata['content_model']))
  
  package_contents.remove('manifest.ini')
  for filename in package_contents:
    if get_file_extension(filename) == 'xml':
      xmldata = xml.etree.ElementTree.fromstring(package.read(filename).decode('utf-8')) 
      iid = False
      identifiers = xmldata.findall('{http://www.loc.gov/mods/v3}identifier')
      for identifier in identifiers:
        if identifier.attrib['type'].lower() == 'iid':
          iid = identifier.text
          if iid != get_file_basename(filename):
            package_errors.append("{0} filename does not match contained IID '{1}'".format(filename, iid))
      iid_exempt_cmodels = get_iid_exempt_cmodels()
      if not iid and package_metadata['content_model'] not in iid_exempt_cmodels:
        package_errors.append("{0} does not contain an IID".format(filename))
    else:
      associated_mods = "{0}.xml".format(get_file_basename(filename))
      if associated_mods not in package_contents:
        package_errors.append("{0} has no associated MODS record".format(filename))

  if len(package_errors) > 0:
    log("Package {0} failed to validate with the following errors: {1}.".format(package_name, ', '.join(package_errors)))
    move_new_s3_package(package_name, 'error')
    return False
  else:
    package_metadata['status'] = 'validated'
    log("Package {0} passed validation check.".format(package_name, ', '.join(package_errors)))
    return package_metadata

def package_preprocess(package_metadata):
  package_metadata['status'] = 'preprocessed'
  package_metadata['batch_id'] = '?'
  return package_metadata

def package_process(package_metadata):
  package_metadata['status'] = 'processed'
  return package_metadata

# Main function
def run():
  running = check_pidfile()
  if not running:
    write_pidfile()
    if not check_new_packages():
      print("AIS ran but didn't find any new packages to process.")
    else:
      package_name = download_oldest_new_package()
      package_metadata = validate_package(package_name)
      if package_metadata:
        print('placeholder')
    delete_pidfile()
    
