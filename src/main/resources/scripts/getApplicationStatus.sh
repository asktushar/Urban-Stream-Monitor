#!/usr/bin/env bash
applicationId=$1
yarn application  -status $applicationId | grep 'State :' | grep -v 'Final' | cut -d ':' -f2 | sed 's\ \\g'