<?php

// Get the current formatted time for log messages
function getTime() {
  return trim(shell_exec('date'));
}

// Selects oldest package in packages directory for processing
// If packages directory contains file "stop", return that instead
function selectPackage() {
  $packages = shell_exec('ls -t /diginole_async_ingest/packages');
  $packages_array = explode("\n", trim($packages));
  if (in_array('stop', $packages_array)) {
    $package = 'stop';
  }
  else {
    $package = end($packages_array);
    if (empty($package)) {
      $package = FALSE;
    }
  }
  return $package;
}

// Check to make sure package has a manifest.json with valid keys
function validatePackage($package_filename) {
  $package_metadata = [];
  $package_metadata['filename'] = $package_filename;
  $package_path = '/diginole_async_ingest/packages/' . $package_filename;
  $manifest_json = shell_exec("unzip -p $package_path manifest.json");
  $manifest_data = json_decode($manifest_json, TRUE);
  if (is_null($manifest_data)) {
    $package_metadata['error'][] = "Cannot decode $package_filename/manifest.json; file is not valid JSON.";
  }
  else {
    if (array_key_exists('submitter_email', $manifest_data)) {
      $package_metadata['submitter_email'] = $manifest_data['submitter_email'];
    }
    else {
      $package_metadata['error'][] = "$package_filename/manifest.json is missing submitter_email key.";
    }
    if (array_key_exists('parent_collection', $manifest_data)) {
      $package_metadata['parent_collection'] = $manifest_data['parent_collection'];
    }
    else {
      $package_metadata['error'][] = "$package_filename/manifest.json is missing parent_collection key.";
    }
    if (array_key_exists('content_model', $manifest_data)) {
      $cmodel_pids = [
        'collection' => 'islandora:collectionCModel',
        'pdf' => 'islandora:sp_pdf',
        'thesis' => 'ir:thesisCModel',
        'citation' => 'ir:citationCModel',
        'basic_image' => 'islandora:sp_basic_image',
        'large_image' => 'islandora:sp_large_image_cmodel',
        'audio' => 'islandora:sp-audioCModel',
        'video' => 'islandora:sp_videoCModel',
        'compound' => 'islandora:compoundCModel',
        'book' => 'islandora:bookCModel',
        'newspaper' => 'islandora:newspaperCModel',
        'newspaper_issue' => 'islandora:newspaperIssueCModel'
      ];
      if (in_array($manifest_data['content_model'], $cmodel_pids)) {
        $package_metadata['content_model'] = $manifest_data['content_model'];
      }
      elseif (array_key_exists($manifest_data['content_model'], $cmodel_pids)) {
        $package_metadata['content_model'] = $cmodel_pids[$manifest_data['content_model']];
      }
      else {
        $package_metadata['error'][] = "$package_filename/manifest.json content_model key ${manifest_data['content_model']} is not a valid cmodel PID or shortname.";
      }
    }
    else {
      $package_metadata['error'][] = "$package_filename/manifest.json is missing content_model key.";
    }
  }
  return $package_metadata;
}

function preprocess_package($package_metadata) {
  $log = [];
  $log['log'][] = getTime() . ": Beginning preprocessing of {$package_metadata['filename']}....";
  $drush_user_info_results = [];
  exec("drush --root=/var/www/html/ user:information {$package_metadata['submitter_email']} --format=csv --fields=uid", $drush_user_info_results['output'], $drush_user_info_results['exit_code']);
  if ($drush_user_info_results['exit_code'] == 0) {
    $submitter_drupal_user_id = trim($drush_user_info_results['output'][0]);
    $log['log'][] = "{$package_metadata['filename']} submitter_email '{$package_metadata['submitter_email']}' is Drupal user $submitter_drupal_user_id.";
  }
  else {
    $submitter_drupal_user_id = 1;
    $log['log'][] = "{$package_metadata['filename']} submitter_email '{$package_metadata['submitter_email']}' could not be matched to an existing Drupal user. Submitting as admin (user 1) instead.";
  }
  shell_exec("cp /diginole_async_ingest/packages/{$package_metadata['filename']} /tmp/{$package_metadata['filename']}; zip -d /tmp/{$package_metadata['filename']} manifest.json");
  $drush_command = "drush --root=/var/www/html/ -u $submitter_drupal_user_id ibsp --type=zip --parent={$package_metadata['parent_collection']} --content_models={$package_metadata['content_model']} --scan_target=/tmp/{$package_metadata['filename']} 2>&1";
  $log['log'][] = "Running drush command: $drush_command";
  $drush_islandora_preprocess_results = [];
  exec($drush_command, $drush_islandora_preprocess_results['output'], $drush_islandora_preprocess_results['exit_code']);
  $batch_id = str_replace('SetId: ', '', $drush_islandora_preprocess_results['output'][0]);
  $batch_id = str_replace('[ok]', '', $batch_id);
  $batch_id = str_replace(' ', '', $batch_id);
  $log['batch_id'] = $batch_id;
  $log['log'][] = "{$package_metadata['filename']} preprocessed, batch_id = $batch_id.";
  shell_exec("rm /tmp/{$package_metadata['filename']}");
  return $log;
}
function process_package($package_metadata) {
  $log = [];
  $log['log'][] = "Processing....";
  return $log;
}
