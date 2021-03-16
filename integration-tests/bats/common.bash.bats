#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    teardown_common
}

@test "common.bash: stashing, setting, and restoring dolt users" {
  stash_current_dolt_user
  [ "$STASHED_DOLT_USER_NAME" = "Bats Tests" ]
  [ "$STASHED_DOLT_USER_EMAIL" = "bats@email.fake" ]

  set_dolt_user "Bats Tests 1" "bats-1@email.fake"
  NAME=`current_dolt_user_name`
  EMAIL=`current_dolt_user_email`
  [ "$NAME" = "Bats Tests 1" ]
  [ "$EMAIL" = "bats-1@email.fake" ]

  restore_stashed_dolt_user
  NAME=`current_dolt_user_name`
  EMAIL=`current_dolt_user_email`
  [ "$NAME" = "Bats Tests" ]
  [ "$EMAIL" = "bats@email.fake" ]
}
