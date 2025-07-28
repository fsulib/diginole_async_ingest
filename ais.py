# Imports

import collections
import configparser
import datetime
import glob
import json
import os
import pathlib
import re
import requests
import subprocess
import sys
import time
import xml.etree.ElementTree
import zipfile

# Variables
silence_output = '2>&1 >/dev/null'
apache_name = os.getenv('APACHE_CONTAINER_NAME')
docker_drush_exec_original = ['docker', 'exec', apache_name, 'bash', '-c']
s3_wait = int(os.getenv('AIS_S3WAIT'))
s3_bucket = os.getenv('AIS_S3BUCKET')
s3_path = "{0}/diginole/ais".format(s3_bucket)
package_path = '/diginole_async_ingest/packages'
pidfile = "/tmp/ais.pid"
headers = {'User-Agent': 'DigiNole Asynchronous Ingest System (AIS)'}
cmodels = {
  'islandora:sp_pdf': ['pdf'],
  'ir:thesisCModel': ['pdf'],
  'ir:citationCModel': ['pdf'],
  'islandora:sp_basic_image': ['gif', 'png', 'jpg', 'jpeg', 'tif', 'tiff'],
  'islandora:sp_large_image_cmodel': ['jpg', 'jpeg', 'tif', 'tiff', 'jp2', 'jpg2'],
  'islandora:sp-audioCModel': ['wav', 'mp3'],
  'islandora:sp_videoCModel': ['mp4', 'mov', 'qt', 'm4v', 'avi', 'mkv'],
  'islandora:binaryObjectCModel': [],
  'islandora:bookCModel': ['jpg', 'jpeg', 'tif', 'tiff', 'jp2', 'jpg2', 'txt', 'shtml', 'pdf'],
  'islandora:newspaperIssueCModel': ['jpg', 'jpeg', 'tif', 'tiff', 'jp2', 'jpg2', 'pdf'],
  'islandora:compoundCModel': [],
}

# Independent Functions
def write_pidfile():
  pid = os.getpid()
  print(pid, file=open(pidfile, 'w'))
  
def delete_pidfile():
  os.system("rm {0}".format(pidfile))
  
def check_pidfile():
  if os.path.isfile(pidfile):
    pid = open(pidfile, "r").read().strip()
    return pid
  else:
    return False

def get_current_time():
  return int(time.time())

def get_package_size(package_name):
  output = os.popen('aws s3 ls --human-readable s3://{0}/new/{1}'.format(s3_path, package_name)).readlines()[0].split()
  size_number = output[2]
  size_measure = output[3]
  size_formatted = '{0}{1}'.format(size_number, size_measure)
  return size_formatted 

def update_diginole_ais_backlog_info():
  backlog_list = os.popen('aws s3 ls s3://{0}/new --recursive'.format(s3_path)).readlines()
  backlog_count = len(backlog_list) - 1
  backlog_size = os.popen('aws s3 ls --summarize --human-readable s3://{0}/new/'.format(s3_path)).readlines()[-1]
  backlog_size = backlog_size.strip().split()

  if int(backlog_count) > 0:
    size_number = backlog_size[2]
    size_measure = backlog_size[3]
    size_formatted = '{0}{1}'.format(size_number, size_measure)
    backlog_string = '\'{0} ({1})\''.format(backlog_count, size_formatted)
  else:
    backlog_string = '0'
    
  drushcmd = "drush --root=/var/www/html vset diginole_ais_process_backlog {0} {1}".format(backlog_string, silence_output)
  docker_drush_exec = docker_drush_exec_original.copy()
  docker_drush_exec.append(drushcmd)
  output = subprocess.check_output(docker_drush_exec)

def write_to_drupal_log(start_time, stop_time, package_name, package_status, message):
  current_time = get_current_time()
  logcmd = 'docker exec {0} bash -c "drush --root=/var/www/html sql-query \\"insert into diginole_ais_log (start, stop, package, status, message) values ({1}, {2}, \'{3}\', \'{4}\', \'{5}\');\\"" {6}'.format(apache_name, start_time, stop_time, package_name, package_status, message, silence_output)
  os.system(logcmd)

def log(message, log_file = False):
  print(message)
  if log_file:
    print(message, file=open("{0}/{1}.log".format(package_path, log_file), 'a'))

def move_s3_file(source, destination):
  os.system('aws s3 mv {0} {1} {2}'.format(source, destination, silence_output))

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
  cmd = "docker exec {0} bash -c 'drush --root=/var/www/html sql-query \"select mail, uid from users;\"'".format(apache_name)
  output = os.popen(cmd).readlines()
  uid = False
  for line in output:
    line = line.strip().split('\t')
    if line[0] == package_metadata['submitter_email']:
      uid = line[1]
  if uid:
    log("{0} manifest.ini submitter email {1} matched to Drupal user {2}.".format(package_metadata['filename'], package_metadata['submitter_email'], uid), log_file = package_metadata['filename'])
  else:
    log("{0} manifest.ini submitter email {1} could not be matched to an existing Drupal user, submitter will be replaced by admin (UID1) instead.".format(package_metadata['filename'], package_metadata['submitter_email']), log_file = package_metadata['filename'])
    uid = 1
  return uid

def get_iid_exempt_cmodels():
  cmdstr = "docker exec {0} bash -c 'drush --root=/var/www/html vget diginole_purlz_exempt_cmodels'".format(apache_name)
  output = os.popen(cmdstr).readlines()[0].lstrip('diginole_purlz_exempt_cmodels: ').lstrip("'").rstrip().rstrip("'")
  return output.split(', ')

def set_diginole_ais_process_status(value):
  drushcmd = "drush --root=/var/www/html vset diginole_ais_process_status {1} {2}".format(apache_name, value, silence_output)
  docker_drush_exec = docker_drush_exec_original.copy()
  docker_drush_exec.append(drushcmd)
  output = subprocess.check_output(docker_drush_exec)

def get_diginole_ais_pause_status():
  drushcmd = "drush --root=/var/www/html vget diginole_ais_pause"
  docker_drush_exec = docker_drush_exec_original.copy()
  docker_drush_exec.append(drushcmd)
  output = subprocess.check_output(docker_drush_exec)
  output = output.decode("utf-8")
  output = output.rstrip()
  output = output.lstrip('diginole_ais_pause: ')
  if output == '1':
    return True
  else:
    return False

def check_if_iid_exists_elsewhere(iid):
  drushcmd = 'docker exec {0} bash -c "drush  --root=/var/www/html php-eval \\"module_load_include(\'inc\', \'diginole_purlz\', \'includes/utilities\'); echo json_encode(diginole_purlz_search_iid(\'{1}\'));\\""'.format(apache_name, iid)
  output = json.loads(os.popen(drushcmd).read())
  return output

def check_if_apache_is_down():
  """Determine apache container status and whether it can produce a meaningful response."""
  status, output = subprocess.getstatusoutput("docker inspect apache")
  if status != 0:
      log("Apache container inspection status code is {str(status)}. This is bad.")
      return True
  # must load subprocess json output
  netinfo = json.loads(output)
  ipaddr = netinfo[0]["NetworkSettings"]["Networks"]["repoman"]["IPAddress"]
  return not requests.get(f"http://{ipaddr}").ok

def check_if_fedora_is_down():
  try:
    eesult = not requests.get('http://fedora.isle.lib.fsu.edu:8080', headers=headers).ok
  except: 
    result = True
  return result

def wait_for_stack_to_stabilize(package_name):
  while check_if_apache_is_down() or check_if_fedora_is_down():
    set_diginole_ais_process_status("Error: Critical services unavailable.")
    if check_if_apache_is_down():
      log("Apache currently unreachable. Waiting...", log_file = False)
    if check_if_fedora_is_down():
      log("Fedora currently unreachable. Waiting...", log_file = False)
    time.sleep(30)
  else:
    current_time = get_current_time()
    package_size = get_package_size(package_name)
    process_status = '{0},{1},{2}'.format(package_name, package_size, current_time)
    set_diginole_ais_process_status(process_status)
    return True

def create_preprocess_package(package_metadata):
  validatable_package_name = package_metadata['filename'] + ".validate"
  preprocess_package_name = package_metadata['filename'] + ".preprocess"
  validatable_package = zipfile.ZipFile("{0}/{1}".format(package_path, validatable_package_name), 'r')
  preprocess_package = zipfile.ZipFile("{0}/{1}".format(package_path, preprocess_package_name), 'w')
  for item in validatable_package.infolist():
    buffer = validatable_package.read(item.filename)
    if item.filename != 'manifest.ini':
        preprocess_package.writestr(item, buffer)
  validatable_package.close()
  preprocess_package.close()
  os.system('rm {0}/{1}'.format(package_path, validatable_package_name))

  if package_metadata['content_model'] in ['islandora:bookCModel', 'islandora:newspaperIssueCModel', 'islandora:compoundCModel']:
    package_basename = get_file_basename(package_metadata['filename'])
    package_folder = '{0}/{1}'.format(package_path, package_basename) 
    os.system('mkdir {0}'.format(package_folder))
    os.system('mv {0}/{1}.preprocess {2}'.format(package_path, package_metadata['filename'], package_folder))
    os.system('unzip {0}/{1}.preprocess -d {0}/ {2}'.format(package_folder, package_metadata['filename'], silence_output))
    os.system('rm {0}/{1}.preprocess'.format(package_folder, package_metadata['filename']))
    package_files = glob.glob("{0}/*".format(package_folder))

    if package_metadata['content_model'] in ['islandora:bookCModel', 'islandora:newspaperIssueCModel']:
      package_pages = []
      for package_file in package_files:
        package_file_filename = package_file.split('/')[-1]
        if get_file_extension(package_file_filename) != 'xml':
          package_pages.append(pathlib.Path(package_file_filename).stem)
      sorted_package_pages = sorted(list(set(package_pages)))
      for index, pagename in enumerate(sorted_package_pages):
        adjusted_index = index + 1
        page_folder = "{0}/{1}".format(package_folder, adjusted_index)
        os.system("mkdir {0}".format(page_folder))
        
        tif_page = "{0}/{1}.tif".format(package_folder, pagename)
        if os.path.exists(tif_page):
          os.system("mv {0} {1}/OBJ.tif".format(tif_page, page_folder))
          
        tiff_page = "{0}/{1}.tiff".format(package_folder, pagename)
        if os.path.exists(tiff_page):
          os.system("mv {0} {1}/OBJ.tif".format(tiff_page, page_folder))
 
        jp2_page = "{0}/{1}.jp2".format(package_folder, pagename)
        if os.path.exists(jp2_page):
          os.system("mv {0} {1}/JP2.jp2".format(jp2_page, page_folder))
          
        jpg_page = "{0}/{1}.jpg".format(package_folder, pagename)
        if os.path.exists(jpg_page):
          os.system("mv {0} {1}/JPG.jpg".format(jpg_page, page_folder))
          
        txt_page = "{0}/{1}.txt".format(package_folder, pagename)
        if os.path.exists(txt_page):
          os.system("mv {0} {1}/OCR.asc".format(txt_page, page_folder))
          
        shtml_page = "{0}/{1}.shtml".format(package_folder, pagename)
        if os.path.exists(shtml_page):
          os.system("mv {0} {1}/HOCR.shtml".format(shtml_page, page_folder))

        pdf_page = "{0}/{1}.pdf".format(package_folder, pagename)
        if os.path.exists(pdf_page):
          os.system("mv {0} {1}/PDF.pdf".format(pdf_page, page_folder))
          
      metadata_filename = glob.glob("{0}/*.xml".format(package_folder))[0].split("/")[-1]
      os.system("mv {0}/{1} {0}/MODS.xml".format(package_folder, metadata_filename))
      os.system("cd {0}; zip -r {1}.preprocess {2} {3}".format(package_path, package_metadata['filename'], package_folder.split('/')[-1], silence_output))
      os.system("rm -rf {0}".format(package_folder))

    if package_metadata['content_model'] in ['islandora:compoundCModel']:
      package_file_filenames = []
      package_ordered_children = package_metadata['compound_children'].replace(' ', '').split(',')
      wrapper_folder = "{0}/{1}".format(package_folder, package_metadata['compound_parent'])
      os.system("mkdir {0}".format(wrapper_folder))
      os.system("mv {0}/*.* {1}".format(package_folder, wrapper_folder))
      for package_ordered_child in package_ordered_children:
        child_folder = "{0}/{1}".format(wrapper_folder, package_ordered_child)
        os.system("mkdir {0}".format(child_folder))
      for package_file in package_files:
        package_file_filename = package_file.split('/')[-1]
        package_file_basename = get_file_basename(package_file_filename)
        package_file_extension = get_file_extension(package_file_filename)
        if package_file_filename == "{0}.xml".format(package_metadata['compound_parent']):
          os.system("mv {0}/{1} {0}/MODS.xml".format(wrapper_folder, package_file_filename))
        elif package_file_basename in package_ordered_children:
          if package_file_extension == 'xml':
            os.system("mv {0}/{1} {0}/{2}/MODS.xml".format(wrapper_folder, package_file_filename, package_file_basename))
          else:
            os.system("mv {0}/{1} {0}/{2}/OBJ.{3}".format(wrapper_folder, package_file_filename, package_file_basename, package_file_extension))
      structure = open('{0}/structure.xml'.format(wrapper_folder), 'w+')
      structure.write('<?xml version="1.0" encoding="utf-8"?>\n')
      structure.write('<islandora_compound_object>\n')
      for package_ordered_child in package_ordered_children:
        structure.write('  <child content="{0}/{1}" />\n'.format(package_metadata['compound_parent'], package_ordered_child))
      structure.write('</islandora_compound_object>\n')
      structure.close()
      preprocess_scan_target = "{0}.preprocess".format(package_folder)
      os.system("mv {0} {1}".format(package_folder, preprocess_scan_target))
      package_metadata['scan_target'] = preprocess_scan_target

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
          log("Failed to validate with the following errors: {0} is not a zip archive.".format(package_name), log_file = package_name)
          move_s3_file("s3://{0}/new/{1}".format(s3_path, package_name), "s3://{0}/error/{1}".format(s3_path, package_name))
          move_s3_file("{0}/{1}.log".format(package_path, package_name), "s3://{0}/error/{1}.log".format(s3_path, package_name))
          log("Package and log data moved to s3://{0}/error/.".format(s3_path), log_file = False)
          write_to_drupal_log(current_time, current_time, package_name, 'Invalid', "Failed to validate with the following errors: {0} is not a zip archive.".format(package_name))
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
  os.system('aws s3 cp s3://{0}/new/{1} {2}/{1} {3}'.format(s3_path, oldest_new_package_name, package_path, silence_output))
  if not os.path.exists("{0}/{1}".format(package_path, oldest_new_package_name)): 
    error_msg = "Error: Unable to download '{0}'. AIS processing halted until problem is addressed.".format(package_name)
    set_diginole_ais_process_status(error_msg)
    sys.exit(error_msg)
  else:
    log("New package {0}/new/{1} detected and downloaded to {2}/{1}.".format(s3_path, oldest_new_package_name, package_path), log_file = oldest_new_package_name)
    return oldest_new_package_name

def validate_package(package_name):
  current_time = get_current_time()
  package_size = get_package_size(package_name)
  process_status = '{0},{1},{2}'.format(package_name, package_size, current_time)
  set_diginole_ais_process_status(process_status)
  wait_for_stack_to_stabilize(package_name)
  package_metadata = {'filename': package_name}
  package_metadata['start_time'] = get_current_time()
  package_errors = []
  exception_error = False 
  validatable_package_name = package_name + ".validate"
  original_package = zipfile.ZipFile("{0}/{1}".format(package_path, package_name), 'r')
  validatable_package = zipfile.ZipFile("{0}/{1}".format(package_path, validatable_package_name), 'w')
  ignored_package_files = ['.DS_Store', 'Thumbs.db', 'thumbs.db']
  for item in original_package.infolist():
    buffer = original_package.read(item.filename)
    if not item.filename.startswith('__MACOSX') and item.filename not in ignored_package_files:
        validatable_package.writestr(item, buffer)
    else:
      log("Superfluous file or subdirectory '{0}' detected, stripping from final ingest package.".format(item.filename), log_file = package_name)
  original_package.close()
  validatable_package.close()
  os.system('rm {0}/{1}'.format(package_path, package_name))
  validatable_package = zipfile.ZipFile("{0}/{1}".format(package_path, validatable_package_name), 'r')
  validatable_package_contents = validatable_package.namelist()
  subfolder_files = []
  for filename in validatable_package_contents:
    splitfilename = filename.split('/')
    if len(splitfilename) > 1:
      if splitfilename[1]:
        subfolder_files.append(filename)
  if len(subfolder_files) > 0:
    joined_subfolder_files = ', '.join(subfolder_files)
    package_errors.append("package contains files in subdirectories: [{0}]".format(joined_subfolder_files))
  else:
    if 'manifest.ini' not in validatable_package_contents:
      package_metadata['submitter_email'] = False
      package_metadata['content_model'] = False
      package_metadata['parent_collection'] = False
      package_errors.append('missing manifest.ini file')
    else:
      manifest = configparser.ConfigParser()
      manifest.read_string(validatable_package.read('manifest.ini').decode('utf-8'))
      if 'package' not in manifest.sections():
        package_errors.append('manifest.ini missing [package] section')
      else:
        if 'submitter_email' in manifest['package'].keys():
          package_metadata['submitter_email'] = manifest['package']['submitter_email'] 
        else: 
          package_metadata['submitter_email'] = False
          package_errors.append('manifest.ini missing submitter_email')
        if 'content_model' in manifest['package'].keys():
          package_metadata['content_model'] = manifest['package']['content_model']
          if package_metadata['content_model'] not in cmodels.keys():
            package_errors.append("manifest.ini content_model {0} is an invalid content model".format(package_metadata['content_model']))
            package_metadata['content_model'] = False
        else: 
          package_metadata['content_model'] = False
          package_errors.append('manifest.ini missing content_model')
        if 'parent_collection' not in manifest['package'].keys(): 
          package_metadata['parent_collection'] = False
          package_errors.append('manifest.ini missing parent_collection')
        else: 
          package_metadata['parent_collection'] = manifest['package']['parent_collection']
          drushcmd = 'drush --root=/var/www/html/ php-eval "echo islandora_object_load(\'{0}\')->id;"'.format(package_metadata['parent_collection'])
          docker_drush_exec = docker_drush_exec_original.copy()
          docker_drush_exec.append(drushcmd)
          output = subprocess.check_output(docker_drush_exec)
          output = output.decode('utf-8').split('\n')
          if output[0] != package_metadata['parent_collection']:
            package_errors.append("manifest.ini parent_collection {0} does not exist".format(package_metadata['parent_collection']))
        if 'register_doi' in manifest['package'].keys():
          package_metadata['register_doi'] = manifest['package']['register_doi']
      if 'ip_embargo' in manifest.sections():
        if 'ip_expiry' in manifest['ip_embargo'].keys():
          package_metadata['ip_expiry'] = manifest['ip_embargo']['ip_expiry'] 
        else: 
          package_errors.append('manifest.ini [ip_embargo] section missing ip_expiry key')
      if 'scholar_embargo' in manifest.sections():
        if 'scholar_expiry' in manifest['scholar_embargo'].keys():
          package_metadata['scholar_expiry'] = manifest['scholar_embargo']['scholar_expiry'] 
        else: 
          package_errors.append('manifest.ini [scholar_embargo] section missing scholar_expiry key')
        if 'scholar_type' in manifest['scholar_embargo'].keys():
          package_metadata['scholar_type'] = manifest['scholar_embargo']['scholar_type'] 
        else: 
          package_errors.append('manifest.ini [scholar_embargo] section missing scholar_type key')
      if 'compound' in manifest.sections():
        if 'parent' in manifest['compound'].keys():
          package_metadata['compound_parent'] = manifest['compound']['parent'] 
        else: 
          package_errors.append('manifest.ini [compound] section missing parent key')
        if 'children' in manifest['compound'].keys():
          package_metadata['compound_children'] = manifest['compound']['children'] 
        else: 
          package_errors.append('manifest.ini [compound] section missing children key')
        if 'pdfmap' in manifest['compound'].keys():
          package_metadata['compound_pdfmap'] = manifest['compound']['pdfmap'] 
      validatable_package_contents.remove('manifest.ini')

    xmlfiles = []
    assetfiles = []
    for filename in validatable_package_contents:
      if get_file_extension(filename) == 'xml':
        xmlfiles.append(filename)
        try:
          xmldata = xml.etree.ElementTree.fromstring(validatable_package.read(filename).decode('utf-8')) 
          iid = False
          identifiers = xmldata.findall('{http://www.loc.gov/mods/v3}identifier')
          for identifier in identifiers:
            if identifier.attrib['type'].lower() == 'iid':
              iid = identifier.text
              if iid != get_file_basename(filename):
                package_errors.append("{0} filename does not match contained IID {1}".format(filename, iid))
              else:
                pids = check_if_iid_exists_elsewhere(iid)
                if len(pids) > 0:
                  pidstring = ", ".join(pids)
                  package_errors.append("{0} filename contains IID {1} already in use by {2}".format(filename, iid, pidstring))
          iid_exempt_cmodels = get_iid_exempt_cmodels()
          if not iid and package_metadata['content_model'] and package_metadata['content_model'] not in iid_exempt_cmodels:
            package_errors.append("{0} does not contain an IID".format(filename))
          if package_metadata['content_model'] and package_metadata['content_model'] == 'islandora:newspaperIssueCModel':
            date_issued_present = False
            origins = xmldata.findall('{http://www.loc.gov/mods/v3}originInfo')
            for origin in origins:
              for element in origin:
                if element.tag == '{http://www.loc.gov/mods/v3}dateIssued':
                  date_issued_present = True
                  if 'encoding' not in element.attrib.keys():
                    package_errors.append("{0} originInfo/dateIssued missing encoding attribute".format(filename))
                  else:
                    if element.attrib['encoding'].lower() not in ['w3cdtf', 'iso8601']:
                      package_errors.append("{0} originInfo/dateIssued encoding attribute is {1}, but is08601 or w3cdtf is required to reflect YYYY-MM-DD format".format(filename, element.attrib['encoding']))
                    else:
                      date_issued = element.text
                      iso_regex = '^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]$'
                      if not re.match(iso_regex, date_issued):
                        package_errors.append("{0} originInfo/dateIssued {1} does not fit iso8601/w3cdtf YYYY-MM-DD format".format(filename, date_issued))
            if not date_issued_present:
              package_errors.append("{0} missing dateIssued element".format(filename))
        except:
          package_errors.append("Error while attempting to parse {0} (see s3://{1}/error/{2}.log for full error output)".format(filename, s3_path, package_metadata['filename']))
          exception_error = {'filename': filename, 'exception': sys.exc_info()}
      else:
        assetfiles.append(filename)
        if package_metadata['content_model'] and package_metadata['content_model'] not in ['islandora:binaryObjectCModel', 'islandora:compoundCModel'] and get_file_extension(filename) not in cmodels[package_metadata['content_model']]:
          package_errors.append("{0} does not have an approved file extension for {1} objects".format(filename, package_metadata['content_model']))
        associated_mods = "{0}.xml".format(get_file_basename(filename))
        if package_metadata['content_model'] and package_metadata['content_model'] not in ['islandora:bookCModel', 'islandora:newspaperIssueCModel'] and associated_mods not in validatable_package_contents:
          package_errors.append("{0} has no associated MODS record".format(filename))
    if len(assetfiles) < 1:
      package_errors.append("{0} has no asset files".format(package_metadata['filename']))
    if len(xmlfiles) < 1:
      package_errors.append("{0} has no XML files".format(package_metadata['filename']))
    elif package_metadata['content_model'] and package_metadata['content_model'] in ['islandora:bookCModel', 'islandora:newspaperIssueCModel'] and len(xmlfiles) > 1:
      package_errors.append("{0} packages should only ever have 1 XML file but {1} has {2} XML files".format(package_metadata['content_model'], package_metadata['filename'], len(xmlfiles)))
  if len(package_errors) > 0:
    invalid_logmsg = "Failed to validate with the following errors: {0}.".format(', '.join(package_errors))
    log(invalid_logmsg, log_file = package_name)
    if exception_error:
      log("Error while attempting to parse {0}: {1}.".format(exception_error['filename'], exception_error['exception']), log_file = package_name)
    move_s3_file("s3://{0}/new/{1}".format(s3_path, package_name), "s3://{0}/error/{1}".format(s3_path, package_name))
    move_s3_file("{0}/{1}.log".format(package_path, package_name), "s3://{0}/error/{1}.log".format(s3_path, package_name))
    os.system("rm {0}/{1} {2}".format(package_path, package_name, silence_output))
    os.system("rm {0}/{1}.validate {2}".format(package_path, package_name, silence_output))
    log("Package and log data moved to s3://{0}/error/.".format(s3_path), log_file = False)
    package_metadata['stop_time'] = get_current_time()
    write_to_drupal_log(package_metadata['start_time'], package_metadata['stop_time'], package_metadata['filename'], 'Invalid', invalid_logmsg)
    set_diginole_ais_process_status("Inactive")
    return False
  else:
    package_metadata['status'] = 'validated'
    log("Passed validation check.".format(package_name), log_file = package_name)
    return package_metadata

def package_preprocess(package_metadata):
  create_preprocess_package(package_metadata) 
  drupaluid = get_drupaluid_from_email(package_metadata)
  if package_metadata['content_model'] == 'islandora:bookCModel':
    drushcmd = "drush --root=/var/www/html/ -u {0} ibbp --type=zip --parent={1} --scan_target={2}/{3}.preprocess --namespace=fsu --output_set_id 2>&1".format(drupaluid, package_metadata['parent_collection'], package_path, package_metadata['filename'])
  elif package_metadata['content_model'] == 'islandora:newspaperIssueCModel':
    drushcmd = "drush --root=/var/www/html/ -u {0} inbp --type=zip --parent={1} --scan_target={2}/{3}.preprocess --namespace=fsu --output_set_id 2>&1".format(drupaluid, package_metadata['parent_collection'], package_path, package_metadata['filename'])
  elif package_metadata['content_model'] == 'islandora:binaryObjectCModel':
    drushcmd = "drush --root=/var/www/html/ -u {0} ibobsp --parent={1} --scan_target={2}/{3}.preprocess --namespace=fsu 2>&1".format(drupaluid, package_metadata['parent_collection'], package_path, package_metadata['filename'])
  elif package_metadata['content_model'] == 'islandora:compoundCModel':
    drushcmd = "drush --root=/var/www/html/ -u {0} icbp --parent={1} --scan_target={2} --namespace=fsu 2>&1".format(drupaluid, package_metadata['parent_collection'], package_metadata['scan_target'])
    os.system("zip -r {0}/{1}.preprocess {2} {3}".format(package_path, package_metadata['filename'], package_metadata['scan_target'], silence_output))
  else:
    drushcmd = "drush --root=/var/www/html/ -u {0} ibsp --type=zip --parent={1} --content_models={2} --scan_target={3}/{4}.preprocess 2>&1".format(drupaluid, package_metadata['parent_collection'], package_metadata['content_model'], package_path, package_metadata['filename'])
  docker_drush_exec = docker_drush_exec_original.copy()
  docker_drush_exec.append(drushcmd)
  wait_for_stack_to_stabilize(package_metadata['filename'])
  try:
    output = subprocess.check_output(docker_drush_exec)
    output = output.decode('utf-8').strip().split()
    package_metadata['status'] = 'preprocessed'
    if package_metadata['content_model'] in ['islandora:bookCModel', 'islandora:newspaperIssueCModel']:
      package_metadata['batch_set_id'] = output[0]
    else:
      package_metadata['batch_set_id'] = output[1]
    log("Preprocessed, assigned Batch Set ID {0}".format(package_metadata['batch_set_id']), log_file =  package_metadata['filename'])
  except subprocess.CalledProcessError as e:
    package_metadata['status'] = 'failed'
    log("Exception caught during preprocessing, failed with {0}".format(sys.exc_info()), log_file = package_metadata['filename'])
    log(e.output.decode('utf-8'), log_file = package_metadata['filename'])
    move_s3_file("s3://{0}/new/{1}".format(s3_path, package_metadata['filename']), "s3://{0}/error/{1}".format(s3_path, package_metadata['filename']))
    move_s3_file("{0}/{1}.preprocess".format(package_path, package_metadata['filename']), "s3://{0}/error/{1}.preprocess".format(s3_path, package_metadata['filename']))
    move_s3_file("{0}/{1}.log".format(package_path, package_metadata['filename']), "s3://{0}/error/{1}.log".format(s3_path, package_metadata['filename']))
    log("Package and log data moved to s3://{0}/error/.".format(s3_path), log_file = False)
    package_metadata['stop_time'] = get_current_time()
    write_to_drupal_log(package_metadata['start_time'], package_metadata['stop_time'], package_metadata['filename'], 'Error', "Error encountered during preprocessing, see s3://{0}/error/{1}.log for full error output.".format(s3_path, package_metadata['filename']))
    set_diginole_ais_process_status("Inactive")
  return package_metadata

def package_ingest(package_metadata):
  if package_metadata['status'] != 'failed':
    drushcmd = "drush --root=/var/www/html/ -u 1 ibi --ingest_set={0} 2>&1".format(package_metadata['batch_set_id'])
    docker_drush_exec = docker_drush_exec_original.copy()
    docker_drush_exec.append(drushcmd)
    wait_for_stack_to_stabilize(package_metadata['filename'])
    try:
      output = subprocess.check_output(docker_drush_exec)
      output = output.decode('utf-8').split('\n')
      pids = []
      loginfo = []
      if output[0].startswith('WD islandora: Failed to ingest object:'):
        package_metadata['status'] = 'failed'
        logstring = "\n".join(output)
        log("Failed to ingest with the following errors:\n{0}".format(logstring), log_file = package_metadata['filename'])
        move_s3_file("s3://{0}/new/{1}".format(s3_path, package_metadata['filename']), "s3://{0}/error/{1}".format(s3_path, package_metadata['filename']))
        move_s3_file("{0}/{1}.preprocess".format(package_path, package_metadata['filename']), "s3://{0}/error/{1}.preprocess".format(s3_path, package_metadata['filename']))
        move_s3_file("{0}/{1}.log".format(package_path, package_metadata['filename']), "s3://{0}/error/{1}.log".format(s3_path, package_metadata['filename']))
        log("Package and log data moved to s3://{0}/error/.".format(s3_path), log_file = False)
        package_metadata['stop_time'] = get_current_time()
        write_to_drupal_log(package_metadata['start_time'], package_metadata['stop_time'], package_metadata['filename'], 'Error', "Error encountered during ingestion, see s3://{0}/error/{1}.log for full error output.".format(s3_path, package_metadata['filename']))
        set_diginole_ais_process_status("Inactive")
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
        package_metadata['status'] = 'ingested'
        log("Ingested, produced PIDs: {0}".format(pidstring), log_file = package_metadata['filename'])
        log("Ingestion produced the following log output:\n{0}".format(logstring), log_file = package_metadata['filename'])
        if package_metadata['content_model'] in ['islandora:bookCModel', 'islandora:newspaperIssueCModel']:
          parent_pid = pids[-1]
          log("Generating FULL_TEXT datastream for parent object '{0}' ".format(parent_pid), log_file = package_metadata['filename'])
          drushcmd = "drush --root=/var/www/html/ -u 1 dbnifi --pid={0}".format(parent_pid)
          docker_drush_exec = docker_drush_exec_original.copy()
          docker_drush_exec.append(drushcmd)
          output = subprocess.check_output(docker_drush_exec)
          output = output.decode('utf-8').split('\n')
          log("Generation of FULL_TEXT datastream finished".format(parent_pid), log_file = package_metadata['filename'])
        if 'ip_expiry' in package_metadata:
          ip_expiry = package_metadata['ip_expiry']
          log("IP embargo with expiry of '{0}' detected in manifest.ini".format(ip_expiry), log_file = package_metadata['filename'])
          for pid in pids:
            if ip_expiry != 'indefinite':
              ip_embargo_cmd = "islandora_ip_embargo_set_embargo('{0}', 2, strtotime('{1}'));".format(pid, ip_expiry)
            else:
              ip_embargo_cmd = "islandora_ip_embargo_set_embargo('{0}', 2);".format(pid)
            module_load_cmd = "module_load_include('inc', 'islandora_ip_embargo', 'includes/utilities');"
            ip_embargo_cmd = module_load_cmd + ip_embargo_cmd
            drushcmd = "drush --root=/var/www/html/ -u 1 eval \"{0}\"".format(ip_embargo_cmd)
            docker_drush_exec = docker_drush_exec_original.copy()
            docker_drush_exec.append(drushcmd)
            output = subprocess.check_output(docker_drush_exec)
            output = output.decode('utf-8').split('\n')
            log("IP embargo with expiry of '{0}' applied to {1}".format(ip_expiry, pid), log_file = package_metadata['filename'])
        if 'scholar_expiry' in package_metadata and 'scholar_type' in package_metadata:
          scholar_expiry = package_metadata['scholar_expiry']
          scholar_type = package_metadata['scholar_type']
          log("Scholar embargo of type '{0}' with an expiry of '{1}' detected from manifest.ini".format(scholar_type, scholar_expiry), log_file = package_metadata['filename'])
          for pid in pids:
            if scholar_type == 'object':
              if scholar_expiry != 'indefinite':
                scholar_embargo_cmd = "islandora_scholar_embargo_set_embargo('{0}', NULL, '{1}');".format(pid, scholar_expiry)
              else:
                scholar_embargo_cmd = "islandora_scholar_embargo_set_embargo('{0}');".format(pid)
            else:
              datastreams = scholar_type.replace(" ", "").split(",")
              for datastream in datastreams:
                if scholar_expiry != 'indefinite':
                  scholar_embargo_cmd = "islandora_scholar_embargo_set_embargo('{0}', array('{1}'), '{2}');".format(pid, datastream, scholar_expiry)
                else:
                  scholar_embargo_cmd = "islandora_scholar_embargo_set_embargo('{0}', array('{1}'));".format(pid, datastream)
            drushcmd = "drush --root=/var/www/html/ -u 1 eval \"{0}\"".format(scholar_embargo_cmd)
            docker_drush_exec = docker_drush_exec_original.copy()
            docker_drush_exec.append(drushcmd)
            output = subprocess.check_output(docker_drush_exec)
            output = output.decode('utf-8').split('\n')
            log("Scholar embargo of type '{0}' with an expiry of '{1}' applied to {2}".format(scholar_type, scholar_expiry, pid), log_file = package_metadata['filename'])
        if 'register_doi' in package_metadata:
          doi = package_metadata['register_doi']
          if len(pids) != 1:
            if package_metadata['content_model'] in ['islandora:bookCModel', 'islandora:newspaperIssueCModel']:
              pid = pids[-1]
              log("Registering DOI for parent object '{0}'.".format(pid), log_file = False)
              drushcmd = "drush --root=/var/www/html/ -u 1 diginole_purlz_register_doi {0} {1}".format(pid, doi)
              docker_drush_exec = docker_drush_exec_original.copy()
              docker_drush_exec.append(drushcmd)
              output = subprocess.check_output(docker_drush_exec)
              output = output.decode('utf-8').split('\n')
            elif package_metadata['content_model'] in ['islandora:compoundCModel']:
              pid = pids[0]
              log("Seeking compound parent of '{0}' for DOI registration.".format(pid), log_file = False)
              drushcmd = "drush --root=/var/www/html/ -u 1 dgcpo --pid={0}".format(pid)
              docker_drush_exec = docker_drush_exec_original.copy()
              docker_drush_exec.append(drushcmd)
              output = subprocess.check_output(docker_drush_exec)
              output = output.decode('utf-8').strip()
              log("'{0}' has compound parent '{1}' for DOI registration.".format(pid, output), log_file = False)
              drushcmd = "drush --root=/var/www/html/ -u 1 diginole_purlz_register_doi {0} {1}".format(pid, doi)
              docker_drush_exec = docker_drush_exec_original.copy()
              docker_drush_exec.append(drushcmd)
              output = subprocess.check_output(docker_drush_exec)
              output = output.decode('utf-8').split('\n')
            else:
              log("DOI '{0}' cannot be registered, multiple PIDs produced on an unsupported cmodel.".format(doi), log_file = package_metadata['filename'])
          else:
            pid = pids[0]
            drushcmd = "drush --root=/var/www/html/ -u 1 diginole_purlz_register_doi {0} {1}".format(pid, doi)
            docker_drush_exec = docker_drush_exec_original.copy()
            docker_drush_exec.append(drushcmd)
            output = subprocess.check_output(docker_drush_exec)
            output = output.decode('utf-8').split('\n')
        move_s3_file("s3://{0}/new/{1}".format(s3_path, package_metadata['filename']), "s3://{0}/done/{1}".format(s3_path, package_metadata['filename']))
        move_s3_file("{0}/{1}.preprocess".format(package_path, package_metadata['filename']), "s3://{0}/done/{1}.preprocess".format(s3_path, package_metadata['filename']))

        move_s3_file("{0}/{1}.log".format(package_path, package_metadata['filename']), "s3://{0}/done/{1}.log".format(s3_path, package_metadata['filename']))
        log("Package and log data moved to s3://{0}/done/.".format(s3_path), log_file = False)
        package_metadata['stop_time'] = get_current_time()
        write_to_drupal_log(package_metadata['start_time'], package_metadata['stop_time'], package_metadata['filename'], 'Success', pidstring)
        set_diginole_ais_process_status("Inactive")
    except subprocess.CalledProcessError as e:
      package_metadata['status'] = 'failed'
      log("Exception caught during ingestion, Batch Set {0} failed with {1}".format(package_metadata['batch_set_id'], sys.exc_info()), log_file = package_metadata['filename'])
      log(e.output.decode('utf-8'), log_file = package_metadata['filename'])
      move_s3_file("s3://{0}/new/{1}".format(s3_path, package_metadata['filename']), "s3://{0}/error/{1}".format(s3_path, package_metadata['filename']))
      move_s3_file("{0}/{1}.preprocess".format(package_path, package_metadata['filename']), "s3://{0}/error/{1}.preprocess".format(s3_path, package_metadata['filename']))
      move_s3_file("{0}/{1}.log".format(package_path, package_metadata['filename']), "s3://{0}/error/{1}.log".format(s3_path, package_metadata['filename']))
      log("Package and log data moved to s3://{0}/error/.".format(s3_path), log_file = False)
      package_metadata['stop_time'] = get_current_time()
      write_to_drupal_log(package_metadata['start_time'], package_metadata['stop_time'], package_metadata['filename'], 'Error', "Error encountered during ingestion, see s3://{0}/error/{1}.log for full error output.".format(s3_path, package_metadata['filename']))
      set_diginole_ais_process_status("Inactive")
  if package_metadata['content_model'] == 'islandora:compoundCModel':
    os.system("rm -rf {0}".format(package_metadata['scan_target']))
  return package_metadata

def process_available_s3_packages():
  update_diginole_ais_backlog_info()
  if get_diginole_ais_pause_status():
      log("AIS paused. Unpause to resume processing available packages.".format(s3_path), log_file = False)
  else:
    if not check_new_packages():
      log("No new packages detected in {0}/new/.".format(s3_path), log_file = False)
    else:
      package_name = download_oldest_new_package()
      package_metadata = validate_package(package_name)
      if package_metadata:
        package_metadata = package_preprocess(package_metadata)
        package_metadata = package_ingest(package_metadata)
      process_available_s3_packages()

# Main function
def run():
  log("AIS triggered.", log_file = False)
  update_diginole_ais_backlog_info()
  pid = check_pidfile()
  if pid:
    log("Another AIS process is already running. Halting execution.", log_file = False)
  else:
    write_pidfile()
    process_available_s3_packages()
    delete_pidfile()
  update_diginole_ais_backlog_info()
