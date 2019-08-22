#!/bin/bash
set -e

tag=$1

echo "Decrypting GPG secret keyring"
openssl aes-256-cbc -K $encrypted_04c98034e207_key -iv $encrypted_04c98034e207_iv -in gpg/secring.asc.enc -out gpg/secring.asc -d

echo "Setting version for project to $tag"
echo "version in ThisBuild := \"$tag\"" > version.sbt

sbt +publishSigned

if [[ ! "${tag}" == *"-SNAPSHOT" ]];then
    echo "Promoting ${tag} as a release"
    sbt sonatypeReleaseAll
fi
