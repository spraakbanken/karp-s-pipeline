# Karp-S pipeline

This is Språkbankens tool for turning structural data, mainly lexicons, into uniform JSON data, optionally augmented with
UD tags. It also prepares and installs data into the [Karp-S backend](https://github.com/spraakbanken/karp-s-backend) used in Språkbankens 
tool [Karp-S](https://spraakbanken.gu.se/karp-s/).

## Documentation

The pipeline is centered around:
- importers - currently [JSONL](https://jsonlines.org/) and some variants of CSV
- modifiers - currently tag conversion (to UD), excluding fields and renaming fields. These can modify schema but also the data, but are currently grouped together.
- exporters - for example, JSONL output and SQL and configuration files for the backend
- installers - for example, install resource in an instance of the Karp-S backend

The main commands that can be invoked are:
- ~~**prepare**~~ - read the data and infer schema and output configuration files (*importers*, *modifiers*)
- **run** - do the needed modifications to each entry and output data in new formats (*modifiers*, *exporters*)
- **install** - runs commands and move files, such as adding data to a database, running a command in anther tool etc. (*installers*)

Note: **prepare** is not implemented as a separate step yet, but the tasks are done when calling **run**.

The pipeline aims to do the following:
- Never save all entries in memory, making it possible to run large datasets
- First pass of the data: infer the schema and order of fields
- Second pass of the data: run modifiers and exporters
- Installers do not read source data

## Future work

- Dependencies - modifiers may need to be run in a specific order to work
- Plugin system
