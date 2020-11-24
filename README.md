pyWWA
=====

This repository is an integration of [pyIEM backed](https://github.com/akrherz/pyIEM) NWS product parsers.  These are intended to be run from [Unidata LDM](https://github.com/Unidata/LDM)'s `pqact` process.

[![Build Status](https://api.travis-ci.com/akrherz/pyWWA.svg)](https://travis-ci.com/github/akrherz/pyWWA)

Python 3.6+ is required to use this code.

Command Line Options
--------------------

The ``pyWWA`` parsers have a common set of command line options that control
their execution.

Short Flag | Long Flag | Description
--- | --- | ---
-d | --disable-dbwrite | Turn off any database writing.  The script still may attempt read access to initialize tables.
-l | --stdout-logging | Emit any log message to stdout.
-u | --utcnow | Provide an ISO-9660 timestamp to the ingestor to denote what the current UTC timestamp is.  This is sometimes necessary for parsing old text products that have ambiguous timestamps (ie METAR).  Defaults to real-time.
-x | --disable-xmpp | Disable any XMPP/Jabber functionality.

Constants Used
--------------

key | value
------------- | -------------
pywwa_email_limit | Number of debug emails an individual ingestor is permitted to send within 60 minutes of time
pywwa_lsr_url | URL Base for the LSR App
pywwa_product_url | URL Base for simple text product viewer
pywwa_river_url | URL Base for the River App
pywwa_vtec_url | URL Base for the VTEC App
pywwa_watch_url | URL Base for the Watch App
