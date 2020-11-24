pyWWA
=====

These are NWS product ingestors and processors.  Not sure how useful this is to
anybody else, but perhaps!  If you have questions, please let me know:

   daryl herzmann
   akrherz@iastate.edu
   515-294-5978

Feel free to do whatever you wish with these, including throwing them in the
trash. Someday, I'll be a real programmer and write a libary that is usable.

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
