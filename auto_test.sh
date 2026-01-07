#!/bin/bash

function test_delay() {
  sed -i 's/^DELAY=.*/DELAY=5/' ../.env
  . test/dev_deploy.sh
  test
  test/test.sh
  download_log

  sed -i 's/^DELAY=.*/DELAY=15/' ../.env
  . test/dev_deploy.sh
  test
  test/test.sh
  download_log

  sed -i 's/^DELAY=.*/DELAY=20/' ../.env
  . test/dev_deploy.sh
  test
  test/test.sh
  download_log
}

function test_shard() {
  sed -i 's/^SHARD_NUM=.*/SHARD_NUM=32/' ../.env
  . test/dev_deploy.sh
  test
  test/test.sh
  download_log

  sed -i 's/^SHARD_NUM=.*/SHARD_NUM=16/' ../.env
  . test/dev_deploy.sh
  test
  test/test.sh
  download_log

  sed -i 's/^SHARD_NUM=.*/SHARD_NUM=8/' ../.env
  . test/dev_deploy.sh
  test
  test/test.sh
  download_log

  sed -i 's/^SHARD_NUM=.*/SHARD_NUM=2/' ../.env
  . test/dev_deploy.sh
  test
  test/test.sh
  download_log
}

function download() {
  export DELAY=25
  . test/dev_deploy.sh
  download_log

  export DELAY=5
  . test/dev_deploy.sh
  download_log

  export DELAY=15
  . test/dev_deploy.sh
  download_log

  export DELAY=20
  . test/dev_deploy.sh
  download_log
}

# test_delay
test_shard