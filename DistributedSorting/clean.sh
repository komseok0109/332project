#!/bin/bash

WORKERS_FILE="/home/white/332project/DistributedSorting/workers.txt"

# workers.txt 파일에서 IP 주소를 읽어와서 루프
while IFS= read -r worker_ip; do
    echo "Deploying to $worker_ip"

    ssh -n white@$worker_ip "
        cd ~ && rm *.sum
    " || echo "Failed to execute git pull on $worker_ip"
done < "$WORKERS_FILE"

echo "Deployment complete"
