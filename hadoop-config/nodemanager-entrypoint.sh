#!/bin/bash
if ! command -v python3 &> /dev/null; then
    sed -i 's|deb.debian.org|archive.debian.org|g; s|security.debian.org|archive.debian.org/debian-security|g' /etc/apt/sources.list
    apt-get update -qq
    apt-get install -y python3 -qq
    ln -sf /usr/bin/python3 /usr/bin/python
fi
exec /entrypoint.sh /run.sh nodemanager
