#!/bin/bash

exec python utils/createTables.py &
exec python runner/main.py