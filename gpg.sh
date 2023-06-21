#! /bin/sh

set -o xtrace
gpg --batch --passphrase="${PROPHECY_GPG_PASSPHRASE}" --pinentry-mode loopback $@
