Snapshot testing for pytest
===============================

Extracted snapshot testing lib for LocalStack.

This project is in a very early stage and will be both restructured and renamed.


## Quickstart

To install the python and other developer requirements into a venv run:

    make install

### Configuration options

There's a few env vars that can be used with this project:

* `TEST_TARGET`: Set to `AWS_CLOUD` to use an externally-deployed instance when running tests.
* `SNAPSHOT_LEGACY_REPORT`: By default set to `0`. Can be set to `1`. This enables the legacy reporting output style.
* `SNAPSHOT_UDPATE`: By default set to `0`. Can be set to `1`. This enables updating the snapshot file rather than comparing with its contents.
* `SNAPSHOT_RAW`: By default set to `0`. Can be set to `1`. This outputs an additional snapshot file which contains the untransformed values.

## Format code

We use black and isort as code style tools.
To execute them, run:

    make format

## Build distribution

To build a wheel and source distribution, simply run

    make dist
