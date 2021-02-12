#!/bin/bash
#!/bin/bash
# copied from: https://github.com/mko-x/docker-clamav/blob/master/alpine/main/bootstrap.sh
set -e

if [[ ! -z "${FRESHCLAM_CONF_FILE}" ]]; then
    echo "[bootstrap] FRESHCLAM_CONF_FILE set, copy to /etc/clamav/freshclam.conf"
    mv /etc/clamav/freshclam.conf /etc/clamav/freshclam.conf.bak
    cp -f ${FRESHCLAM_CONF_FILE} /etc/clamav/freshclam.conf
fi

if [[ ! -z "${CLAMD_CONF_FILE}" ]]; then
    echo "[bootstrap] CLAMD_CONF_FILE set, copy to /etc/clamav/clam.conf"
    mv /etc/clamav/clamd.conf /etc/clamav/clamd.conf.bak
    cp -f ${CLAMD_CONF_FILE} /etc/clamav/clamd.conf
fi

MAIN_FILE="/var/lib/clamav/main.cvd"

if [ ! -f ${MAIN_FILE} ]; then
    echo "[bootstrap] Initial clam DB download."
    /usr/bin/freshclam
fi

echo "[bootstrap] Schedule freshclam DB updater."
/usr/bin/freshclam -d -c 6

echo "[bootstrap] Run clamav daemon..."
/usr/sbin/clamd -c /etc/clamav/clam.conf
echo "[bootstrap] process the files!"
java -jar /clamav-1.0.0-SNAPSHOT.jar