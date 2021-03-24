<?php

function preprocess_package($package_metadata) {
  $log = [];
  $log['log'][] = getTime() . ": Beginning preprocessing of {$package_metadata['filename']}....";
  $drush_user_info_results = [];
  exec("drush --root=/var/www/html/ user:information {$package_metadata['submitter_email']} --format=csv --fields=uid 2>&1", $drush_user_info_results['output'], $drush_user_info_results['exit_code']);
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
  $batch_id = log_strip(str_replace('SetId: ', '', $drush_islandora_preprocess_results['output'][0]));
  $log['batch_id'] = $batch_id;
  $log['log'][] = "{$package_metadata['filename']} preprocessed, batch_id = $batch_id.";
  return $log;
}
function process_package($package_metadata) {
  $log = [];
  $drush_islandora_process_results = [];
  $drush_command = "drush --root=/var/www/html/ -v -u 1 ibi 2>&1";
  exec($drush_command, $drush_islandora_process_results['output'], $drush_islandora_process_results['exit_code']);
  $log['log'] = array_map('log_strip', $drush_islandora_process_results['output']);
  shell_exec("rm /tmp/{$package_metadata['filename']}");
  return $log;
}
