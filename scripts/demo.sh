#!/bin/sh
set -eu

curl --fail-with-body \
  --request POST http://localhost:8080/jobs \
  --header 'Content-Type: application/json' \
  --data '{"job_id":"onboard-1042","tenant_id":"acme-eu","operation":"tenant_onboarding","input":{"force_failure":false}}'

curl --fail-with-body --request GET http://localhost:8080/admin/dead-letters
