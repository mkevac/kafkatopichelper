# kafkatopichelper

This is a quick and dirty utility that can help with:

* Listing topics that exist in Kafka
* Seeing topic configuration
* Altering topic configuration

## Installation

Same as any other Go utility

## Usage

Run utility with `-h` to see available commands and options.

## Careful

I don't know why, but configuration altering in sarama kafka library (and seems that in
Kafka itself too) is very strange. When you set some parameters, other parameters are overwritten
with default values.

kafkatopichelper set mode updates only requested params ie:
- requests current topic configuration
- sets parameters, specified in command line,
  to current topic configuration
- updates topic config
