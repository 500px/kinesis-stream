#!/bin/bash
set -e

tag=$1

echo "Setting version for project to $tag"
echo "version in ThisBuild := \"$tag\"" > version.sbt

sbt publish
