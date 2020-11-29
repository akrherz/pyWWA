pyWWA
=====

This repository is an integration of [pyIEM backed](https://github.com/akrherz/pyIEM) NWS product parsers.  These are intended to be run from [Unidata LDM](https://github.com/Unidata/LDM)'s `pqact` process.

Python 3.6+ is required to use this code.

Command Line Options
--------------------

The ``pyWWA`` parsers have a common set of command line options that control
their execution.

Short Flag | Long Flag | Description
--- | --- | ---
-d | --disable-dbwrite | Turn off any database writing.  The script still may attempt read access to initialize tables.
-e | --disable-email | Disable sending any emails.
-l | --stdout-logging | Emit any log message to stdout.
-u | --utcnow | Provide an ISO-9660 timestamp to the ingestor to denote what the current UTC timestamp is.  This is sometimes necessary for parsing old text products that have ambiguous timestamps (ie METAR).  Defaults to real-time.
-s | --shutdown-delay | The number of seconds to wait before attempting to shutdown the process when STDIN is closed.  Default is 5.
-x | --disable-xmpp | Disable any XMPP/Jabber functionality.

Constants Used
--------------

key | default | description
--- | ---- | ---
pywwa_dedup | false | Set to `true` for pyWWA/pyLDM to dedup products.
pywwa_email_limit | 10 | Number of debug emails an individual ingestor is permitted to send within 60 minutes of time
pywwa_lsr_url | - | URL Base for the LSR App
pywwa_metar_url | - | URL Base for METAR reports.
pywwa_product_url | - | URL Base for simple text product viewer
pywwa_river_url | - | URL Base for the River App
pywwa_vtec_url | - | URL Base for the VTEC App
pywwa_watch_url | - | URL Base for the Watch App

Database Information
--------------------

This repository assumes a backend database schema and nomeclature as provided by the [iem-database repo](https://github.com/akrherz/iem-database).  The default logic is for when a database connection is requested by a given name, the library attempts to connect to a database by the same `name` on a hostname called `iemdb-name.local`.  Passwords should be set by the standard `~/.pgpass` file.

The `settings.json` file provides a means to override this behaviour.  For example, if your "postgis" database is actually called "henry" and on a server named "pgserv", you would set the following in the `settings.json` file:

```json
{
    "postgis": {
        "database": "henry",
        "host": "pgserv"
    }
}
```

Logging
-------

These parsers emit logs to the syslog `LOCAL2` facility via a wild mixture of
Twisted Python and Stdlib Python log statements.
