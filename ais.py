# Imports
import collections
import configparser
import datetime
import glob
import json
import os
import subprocess
import time
import xml.etree.ElementTree
import zipfile


# Variables
silence_output = '2>&1 >/dev/null'
apache_name = os.getenv('APACHE_CONTAINER_NAME')
drush_exec = ['docker', 'exec', apache_name, 'bash', '-c']
s3_wait = int(os.getenv('AIS_S3WAIT'))
s3_bucket = os.getenv('AIS_S3BUCKET')
s3_path = "{0}/diginole/ais".format(s3_bucket)
package_path = '/diginole_async_ingest/packages'
pidfile = "/tmp/ais.pid"
cmodels = [
  'islandora:sp_pdf',
  'ir:thesisCModel',
  'ir:citationCModel',
  'islandora:sp_basic_image',
  'islandora:sp_large_image_cmodel',
  'islandora:sp-audioCModel',
  'islandora:sp_videoCModel',
  'islandora:binaryObjectCModel',
  #'islandora:compoundCModel',
  #'islandora:bookCModel',
  #'islandora:newspaperCModel',
  #'islandora:newspaperIssueCModel'
]


# Independent Functions
def write_pidfile():
  pid = os.getpid()
  print(pid, file=open(pidfile, 'w'))
  
def delete_pidfile():
  os.system("rm {0}".format(pidfile))
  
def check_pidfile():
  log("Checking to see if any other AIS processes are currently running...", drupal_report = False, log_file = False)
  if os.path.isfile(pidfile):
    pid = open(pidfile, "r").read().strip()
    log("Another AIS process (pid:{0}) is currently running.".format(pid), drupal_report = False, log_file = False)
    return pid
  else:
    log("No other AIS processes detected.", drupal_report = False, log_file = False)
    return False

def get_current_time():
  return int(time.time())

def log(message, drupal_report = False, log_file = False):
  print(message)
  if drupal_report:
    current_time = get_current_time()
    logcmd = 'docker exec {0} bash -c "drush --root=/var/www/html sql-query \\"insert into diginole_ais_log (time, message) values ({1}, \'{2}\');\\"" {3}'.format(apache_name, current_time, message, silence_output)
    os.system(logcmd)
  if log_file:
    print(message, file=open("{0}/{1}.log".format(package_path, log_file), 'a'))

def move_s3_file(source, destination):
  log("Moving {0} {1}...".format(source, destination), drupal_report = False, log_file = False)
  os.system('aws s3 mv {0} {1} {2}'.format(source, destination, silence_output))

def delete_downloaded_package(package):
  log("Deleting {0}/{1}...".format(package_path, package), drupal_report = False, log_file = package)
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

def get_drupaluid_from_email(package_metadata):
  log("Getting Drupal user ID for submitter...", drupal_report = False, log_file = False) 
  cmd = "docker exec {0} bash -c 'drush --root=/var/www/html sql-query \"select mail, uid from users;\"'".format(apache_name)
  output = os.popen(cmd).readlines()
  uid = False
  for line in output:
    line = line.strip().split('\t')
    if line[0] == package_metadata['submitter_email']:
      uid = line[1]
  if uid:
    log("{0} manifest.ini submitter email {1} matched to Drupal user {2}.".format(package_metadata['filename'], package_metadata['submitter_email'], uid), drupal_report = False, log_file = package_metadata['filename'])
  else:
    log("{0} manifest.ini submitter email {1} could not be matched to an existing Drupal user, submitter will be replaced by admin (UID1) instead.".format(package_metadata['filename'], package_metadata['submitter_email']), drupal_report = True, log_file = package_metadata['filename'])
    uid = 1
  return uid

def get_iid_exempt_cmodels():
  cmdstr = "docker exec {0} bash -c 'drush --root=/var/www/html vget diginole_purlz_exempt_cmodels'".format(apache_name)
  output = os.popen(cmdstr).readlines()[0].lstrip('diginole_purlz_exempt_cmodels: ').lstrip("'").rstrip().rstrip("'")
  return output.split(', ')

def create_preprocess_package(package_name):
  os.system("zip -d {0}/{1} manifest.ini {2}".format(package_path, package_name, silence_output))
  os.system("mv {0}/{1} {0}/{1}.preprocess".format(package_path, package_name))


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
          log("New package {0}/new/{1} detected, but is not a zip file. Package moved to {0}/error/{1}.".format(s3_path, package_name), drupal_report = True, log_file = package_name)
          move_s3_file("s3://{0}/new/{1}".format(s3_path, package_name), "s3://{0}/error/{1}".format(s3_path, package_name))
          move_s3_file("{0}/{1}.log".format(package_path, package_name), "s3://{0}/error/{1}.log".format(s3_path, package_name))
        else:
          package_key = str(package_mod_timestamp)
          packages[package_key] = package_name
  sorted_packages = collections.OrderedDict(sorted(packages.items()))
  sorted_packages_list = list(sorted_packages.items())
  return sorted_packages_list

def check_new_packages():
  log("Checking for new packages to download...", drupal_report = False, log_file = False)
  packages = list_new_packages()
  if len(packages) > 0:
    return True
  else:
    return False

def download_oldest_new_package():
  packages = list_new_packages()
  oldest_new_package = packages[0]
  oldest_new_package_name = oldest_new_package[1]
  os.system('aws s3 cp s3://{0}/new/{1} {2}/{1} {3}'.format(s3_path, oldest_new_package_name, package_path, silence_output))
  log("New package {0}/new/{1} detected and downloaded to {2}/{1}.".format(s3_path, oldest_new_package_name, package_path), drupal_report = True, log_file = oldest_new_package_name)
  return oldest_new_package_name

def validate_package(package_name):
  log("Validating {0}...".format(package_name), drupal_report = False, log_file = False)
  package_metadata = {'filename': package_name}
  package_errors = []
  os.system("zip -d {0}/{1} __MACOSX/\* {2}".format(package_path, package_name, silence_output))
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
      package_metadata['content_model'] = manifest['package']['content_model'] if 'content_model' in manifest['package'].keys() else package_errors.append('manifest.ini missing content_model')
      if package_metadata['content_model'] not in cmodels:
        package_errors.append("manifest.ini content_model {0} is an invalid content model".format(package_metadata['content_model']))
      if 'parent_collection' not in manifest['package'].keys(): 
        package_errors.append('manifest.ini missing parent_collection')
      else: 
        package_metadata['parent_collection'] = manifest['package']['parent_collection']
        drushcmd = 'drush --root=/var/www/html/ php-eval "echo islandora_object_load(\'{0}\')->id;"'.format(package_metadata['parent_collection'])
        drush_parent_check = drush_exec.copy()
        drush_parent_check.append(drushcmd)
        output = subprocess.check_output(drush_parent_check)
        output = output.decode('utf-8').split('\n')
        if output[0] != package_metadata['parent_collection']:
          package_errors.append("manifest.ini parent_collection {0} does not exist".format(package_metadata['parent_collection']))
    package_contents.remove('manifest.ini')
  subfolder_files = []
  for filename in package_contents:
    splitfilename = filename.split('/')
    if len(splitfilename) > 1:
      if splitfilename[1]:
        subfolder_files.append(filename)
    elif get_file_extension(filename) == 'xml':
      xmldata = xml.etree.ElementTree.fromstring(package.read(filename).decode('utf-8')) 
      iid = False
      identifiers = xmldata.findall('{http://www.loc.gov/mods/v3}identifier')
      for identifier in identifiers:
        if identifier.attrib['type'].lower() == 'iid':
          iid = identifier.text
          if iid != get_file_basename(filename):
            package_errors.append("{0} filename does not match contained IID {1}".format(filename, iid))
      iid_exempt_cmodels = get_iid_exempt_cmodels()
      if not iid and package_metadata['content_model'] not in iid_exempt_cmodels:
        package_errors.append("{0} does not contain an IID".format(filename))
    else:
      associated_mods = "{0}.xml".format(get_file_basename(filename))
      if associated_mods not in package_contents:
        package_errors.append("{0} has no associated MODS record".format(filename))
  if len(subfolder_files) > 0:
    joined_subfolder_files = ', '.join(subfolder_files)
    package_errors.append("package contains files in subdirectories: [{0}]".format(joined_subfolder_files))
  if len(package_errors) > 0:
    log("Package {0} failed to validate with the following errors: {1}.".format(package_name, ', '.join(package_errors)), drupal_report = True, log_file = package_name)
    move_s3_file("s3://{0}/new/{1}".format(s3_path, package_name), "s3://{0}/error/{1}".format(s3_path, package_name))
    move_s3_file("{0}/{1}.log".format(package_path, package_name), "s3://{0}/error/{1}.log".format(s3_path, package_name))
    os.system("rm {0}/{1} {2}".format(package_path, package_name, silence_output))
    log("{0} and {0}.log have been moved to {1}/error/.".format(package_name, s3_path), drupal_report = True, log_file = False)
    return False
  else:
    package_metadata['status'] = 'validated'
    log("Package {0} passed validation check.".format(package_name), drupal_report = True, log_file = package_name)
    return package_metadata

def package_preprocess(package_metadata):
  log("Preprocessing {0}...".format(package_metadata['filename']), drupal_report = False, log_file = False)
  create_preprocess_package(package_metadata['filename']) 
  drupaluid = get_drupaluid_from_email(package_metadata)
  if package_metadata['content_model'] == 'islandora:binaryObjectCModel':
    drushcmd = "drush --root=/var/www/html/ -u {0} ibobsp --parent={1} --scan_target={2}/{3}.preprocess 2>&1".format(drupaluid, package_metadata['parent_collection'], package_path, package_metadata['filename'])
  else:
    drushcmd = "drush --root=/var/www/html/ -u {0} ibsp --type=zip --parent={1} --content_models={2} --scan_target={3}/{4}.preprocess 2>&1".format(drupaluid, package_metadata['parent_collection'], package_metadata['content_model'], package_path, package_metadata['filename'])
  drush_preprocess_exec = drush_exec.copy()
  drush_preprocess_exec.append(drushcmd)
  output = subprocess.check_output(drush_preprocess_exec)
  output = output.decode('utf-8').strip().split()
  package_metadata['status'] = 'preprocessed'
  package_metadata['batch_set_id'] = output[1]
  return package_metadata

def package_process(package_metadata):
  log("Processing {0}...".format(package_metadata['filename']), drupal_report = False, log_file = False)
  drushcmd = "drush --root=/var/www/html/ -u 1 ibi --ingest_set={0} 2>&1".format(package_metadata['batch_set_id'])
  drush_process_exec = drush_exec.copy()
  drush_process_exec.append(drushcmd)
  output = subprocess.check_output(drush_process_exec)
  output = output.decode('utf-8').split('\n')
  pids = []
  loginfo = []
  if output[0].startswith('WD islandora: Failed to ingest object:'):
    package_metadata['status'] = 'failed'
    logstring = "\n".join(output)
    log("{0} failed to ingest, see {1}/error/{0}.log for more information.".format(package_metadata['filename'], s3_path), drupal_report = True, log_file = False)
    log("{0} failed to ingest with the following errors:\n{1}".format(package_metadata['filename'], logstring), drupal_report = False, log_file = package_metadata['filename'])
    move_s3_file("s3://{0}/new/{1}".format(s3_path, package_metadata['filename']), "s3://{0}/error/{1}".format(s3_path, package_metadata['filename']))
    move_s3_file("{0}/{1}.preprocess".format(package_path, package_metadata['filename']), "s3://{0}/error/{1}.preprocess".format(s3_path, package_metadata['filename']))
    move_s3_file("{0}/{1}.log".format(package_path, package_metadata['filename']), "s3://{0}/error/{1}.log".format(s3_path, package_metadata['filename']))
  else:
    for line in output:
      if line.startswith('Ingested'):
        pids.append(line.split()[1].rstrip('.'))
      elif line.startswith('Processing complete;') or line.startswith('information.') or line == '':
        pass  
      else:
        loginfo.append(line)
    pidstring = ", ".join(pids)
    logstring = "\n".join(loginfo)
    package_metadata['status'] = 'processed'
    log("{0} processed, produced PIDs: {1}".format(package_metadata['filename'], pidstring), drupal_report = True, log_file = package_metadata['filename'])
    log("{0} processing produced the following log output:\n{1}".format(package_metadata['filename'], logstring), drupal_report = False, log_file = package_metadata['filename'])
    move_s3_file("s3://{0}/new/{1}".format(s3_path, package_metadata['filename']), "s3://{0}/done/{1}".format(s3_path, package_metadata['filename']))
    move_s3_file("{0}/{1}.preprocess".format(package_path, package_metadata['filename']), "s3://{0}/done/{1}.preprocess".format(s3_path, package_metadata['filename']))
    move_s3_file("{0}/{1}.log".format(package_path, package_metadata['filename']), "s3://{0}/done/{1}.log".format(s3_path, package_metadata['filename']))
  return package_metadata

def process_available_s3_packages():
  if not check_new_packages():
    log("No new packages detected in {0}/new/.".format(s3_path), drupal_report = False, log_file = False)
  else:
    package_name = download_oldest_new_package()
    package_metadata = validate_package(package_name)
    if package_metadata:
      package_metadata = package_preprocess(package_metadata)
      package_metadata = package_process(package_metadata)
    process_available_s3_packages()


# Main function
def run():
  log("Executing main AIS process...", drupal_report = False, log_file = False)
  pid = check_pidfile()
  if pid:
    log("Halting to allow original AIS process to continue.", drupal_report = False, log_file = False)
  else:
    write_pidfile()
    process_available_s3_packages()
    delete_pidfile()
  log("AIS main process completed.", drupal_report = False, log_file = False)
