#!/bin/bash

# considering that you have git clone from repo and stored into /tmp folder

# add user to run service 'locationtracker'
sudo useradd locationtracker -s /sbin/nologin -M

# move service file
#cd /tmp/Fleet-Location/deployment/dev

sudo cp locationtracker-processor.service /etc/systemd/system/
sudo chmod 755 /etc/systemd/system/locationtracker-processor.service

echo "service moved"

# copy source code to go folder
#cd /tmp/Fleet-Location
rm -rf /home/howlun_par/go/src/github.com/howlun/sarama-go-test/*
cp -r * /home/howlun_par/go/src/github.com/howlun/sarama-go-test/
cd /home/howlun_par/go/src/github.com/howlun/sarama-go-test
go build

echo "source build"

# enable and start the service
sudo systemctl daemon-reload

sudo systemctl enable locationtracker-processor
sudo systemctl restart locationtracker-processor

echo "deployment complete"