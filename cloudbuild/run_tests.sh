#!/bin/bash
set -e
source env/bin/activate

# Temporary workaround: Disable mTLS for GCE Metadata Server discovery to avoid
# transport and SSL verification errors on mTLS-enabled VMs. This ensures
# stability across all Google SDKs while library-level mTLS fixes are finalized.
# This is added to support the versioned tests
export GCE_METADATA_MTLS_MODE=none

# Common Exports
export STORAGE_EMULATOR_HOST=https://storage.googleapis.com
export GCSFS_TEST_PROJECT=${PROJECT_ID}
export GCSFS_TEST_KMS_KEY=projects/${PROJECT_ID}/locations/${REGION}/keyRings/${KEY_RING}/cryptoKeys/${KEY_NAME}
export GOOGLE_CLOUD_PROJECT=${PROJECT_ID}

# Pytest Arguments
ARGS=(
  -vv
  -s
  "--log-format=%(asctime)s %(levelname)s %(message)s"
  "--log-date-format=%H:%M:%S"
  --color=no
)

echo "--- Running Test Suite: ${TEST_SUITE} ---"

case "$TEST_SUITE" in
  "standard")
    export GCSFS_TEST_BUCKET="gcsfs-test-standard-${SHORT_BUILD_ID}"
    export GCSFS_TEST_VERSIONED_BUCKET="gcsfs-test-versioned-${SHORT_BUILD_ID}"
    pytest "${ARGS[@]}" gcsfs/tests/test_dummy_cloudbuild.py
    ;;

  "zonal")
    export GCSFS_TEST_BUCKET="gcsfs-test-standard-for-zonal-${SHORT_BUILD_ID}"
    export GCSFS_ZONAL_TEST_BUCKET="gcsfs-test-zonal-${SHORT_BUILD_ID}"
    export GCSFS_HNS_TEST_BUCKET="gcsfs-test-zonal-${SHORT_BUILD_ID}"
    ulimit -n 4096
    export GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT='true'
    pytest "${ARGS[@]}" gcsfs/tests/test_dummy_cloudbuild.py
    ;;

  "hns")
    export GCSFS_TEST_BUCKET="gcsfs-test-hns-${SHORT_BUILD_ID}"
    export GCSFS_ZONAL_TEST_BUCKET="gcsfs-test-hns-${SHORT_BUILD_ID}"
    export GCSFS_HNS_TEST_BUCKET="gcsfs-test-hns-${SHORT_BUILD_ID}"
    export GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT='true'
    pytest "${ARGS[@]}" gcsfs/tests/test_dummy_cloudbuild.py
    ;;

  "zonal-core")
    export GCSFS_TEST_BUCKET="gcsfs-test-zonal-core-${SHORT_BUILD_ID}"
    export GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT='true'
    ulimit -n 4096
    pytest "${ARGS[@]}" gcsfs/tests/test_dummy_cloudbuild.py
    ;;
esac
