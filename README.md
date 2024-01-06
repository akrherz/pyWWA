# Python Watch, Warning, Advisory, and more Parsers (pywwa)

This repository is an integration of
[pyIEM backed](https://github.com/akrherz/pyIEM) NWS product parsers. These
are intended to be run from [Unidata LDM](https://github.com/Unidata/LDM)'s
`pqact` process.  This repo is used by the
[IEM](https://mesonet.agron.iastate.edu) to ingest NWS products and data.

Python 3.9+ is supported by this library.

## Installation

Unfortunately, the dependency needs for this repo are very large due to the
transient dependcies found with ``pyIEM``.  If you are able to use conda-forge,
your life will be much simplier.

An opinionated installation is to clone this repo in the LDM user's `$HOME`
directory and then update LDM's pqact.datadir-path setting
(found in the `registry.xml`) to point to LDM's home directory.
It is then suggested that you create sym links from the LDM user's `etc/`
directory to the `pqact.d` files found within this repo.  For example:

```bash
cd ~/etc
ln -s ../pyWWA/pqact.d/pqact_iemingest.conf
```

This suggestion allows LDM's pqact state files to reside in a directory
outside the pyWWA repo tree.  The `pyWWA` pqact files assume that the above
has been done, such that `pqact` searches the LDM `$HOME` directory for the
`pyWWA` folder.

## Command Line Options

The ``pyWWA`` parsers have a common set of command line options that control
their execution.

Short Flag | Long Flag | Description
--- | --- | ---
-c | --custom-arg | Pass arbitrary strings to the parser. (pywwa-parser-shef)
-d | --disable-dbwrite | Turn off any database writing.  The script still may attempt read access to initialize tables.
-e | --disable-email | Disable sending any emails.
-l | --stdout-logging | Emit any log message to stdout.
-u | --utcnow | Provide an ISO-8601 timestamp to the ingestor to denote what the current UTC timestamp is.  This is sometimes necessary for parsing old text products that have ambiguous timestamps (ie METAR).  Defaults to real-time.
-s | --shutdown-delay | The number of seconds to wait before attempting to shutdown the process when STDIN is closed.  Default is 5.
-x | --disable-xmpp | Disable any XMPP/Jabber functionality.

## Constants Used

key | default | description
--- | ---- | ---
pywwa_dedup | false | Set to `true` for pyWWA to dedup products.
pywwa_email_limit | 10 | Number of debug emails an individual ingestor is permitted to send within 60 minutes of time
pywwa_lsr_url | - | URL Base for the LSR App
pywwa_metar_url | - | URL Base for METAR reports.
pywwa_product_url | - | URL Base for simple text product viewer
pywwa_river_url | - | URL Base for the River App
pywwa_vtec_url | - | URL Base for the VTEC App
pywwa_watch_url | - | URL Base for the Watch App

## Database Information

This repository assumes a backend database schema and nomeclature as provided
by the [iem-database repo](https://github.com/akrherz/iem-database).  The
default logic is for when a database connection is requested by a given name,
the library attempts to connect to a database by the same `name` on a hostname
called `iemdb-name.local`.  Passwords should be set by the standard `~/.pgpass`
file.

The `settings.json` file provides a means to override this behaviour.  For
example, if your "postgis" database is actually called "henry" and on a server
named "pgserv", you would set the following in the `settings.json` file:

```json
{
    "postgis": {
        "database": "henry",
        "host": "pgserv"
    }
}
```

## Logging

These parsers emit logs to the syslog `LOCAL2` facility via a wild mixture of
Twisted Python and Stdlib Python log statements.

## Locating the settings.json file

This is a bit of a hack yet within the codebase, but some crude logic is used
attempting to figure out where the `pyWWA/settings.json` file resides.  Since
the various scripts could be getting executed from various current working
directories, the code checks the following locations.

1. The file `pyWWA/settings.json` exists from the `cwd`

2. The `cwd` of the process.

3. One directory up from the calling script.

4. Two directories up from the calling script.

## Development Notes

The LDM pqact files need to use `<tabs>`, so if you are using vscode, ensure
that the editorconfig extension is installed.
