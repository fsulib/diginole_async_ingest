<?php

include 'functions.php';

$log = [];

$log[] = getTime() . ": process-package.php triggered, determining next package to process from packages directory...";
$select_package_output = selectPackage();

if ($select_package_output) {
  if ($select_package_output == 'stop') {
    $log[] = "Stop file found in packages directory. Halting processing until stop file is removed.";
  }
  else {
    $package_filename = $select_package_output;
    $log[] = "Oldest package in packages directory determined to be $package_filename.";
  }
}
else {
    $log[] = "No packages found in package directory.";
}

if ($package_filename) {
  $log[] = "Validating $package_filename...";
  $validate_package_result = validatePackage($package_filename);
  if (array_key_exists('error', $validate_package_result)) {
    $validation_error_count = count($validate_package_result['error']);
    $log[] = "$validation_error_count errors found validating $package_filename...";
    foreach ($validate_package_result['error'] as $error) {
      $log[] = $error;
    }
  }
  else {
    $package_metadata = $validate_package_result;
    $log[] = "$package_filename validated!";
    $log[] = "$package_filename submitter_email: {$package_metadata['submitter_email']}.";
    $log[] = "$package_filename content_model: {$package_metadata['content_model']}.";
    $log[] = "$package_filename parent_collection: {$package_metadata['parent_collection']}.";
  }

}

if ($package_metadata) {
  $preprocess_output = preprocess_package($package_metadata);
  if (array_key_exists('error', $preprocess_output)) {
    $preprocess_error_count = count($preprocess_output['error']);
    $log[] = "$preprocess_error_count errors found preprocessing $package_filename...";
    foreach ($preprocess_output['error'] as $error) {
      $log[] = $error;
    }
  }
  else {
    $log = array_merge($log, $preprocess_output['log']);
    $package_metadata['batch_id'] = $preprocess_output['batch_id'];
    $log[] = getTime() . ": Preprocessing {$package_metadata['filename']} successful!";
  }
}

if (array_key_exists('batch_id', $package_metadata)) {
  $process_output = process_package($package_metadata);
  if (array_key_exists('error', $process_output)) {
    $process_error_count = count($process_output['error']);
    $log[] = "$process_error_count errors found processing $package_filename...";
    foreach ($process_output['error'] as $error) {
      $log[] = $error;
    }
  }
  else {
    $log = array_merge($log, $process_output['log']);
    $log[] = getTime() . ": Preprocessing {$package_metadata['filename']} successful!";
  }
}

$log[] = getTime() . ": process-package.php finished.";

print_r($log);
