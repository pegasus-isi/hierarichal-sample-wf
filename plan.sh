#!/usr/bin/env bash

DIR=$(cd $(dirname $0) && pwd)

if [ $# -ne 1 ]; then
    echo "Usage: $0 WORKFLOW"
    exit 1
fi

WORKFLOW=$1

#planning the workflow
pegasus-plan --conf pegasus.properties \
    --dir $DIR/submit \
    --sites condorpool \
    --output-site local \
    --cleanup leaf \
    --force \
    $WORKFLOW
    
