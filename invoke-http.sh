#!/bin/bash -e

curl -X POST http://10.10.13.242:80/infer -H "Content-type: text/plain" -d 'George Washington was president.'
