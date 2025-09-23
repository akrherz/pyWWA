<!-- markdownlint-configure-file {"MD024": { "siblings_only": true } } -->
# Changelog

All notable changes to this library are documented in this file.

## **1.7.1** (23 Sep 2025)

### API Changes

- Bump minimum python to 3.10

### New Features

- Create Faked AFOS identifiers for CDUS27 `DSM`.
- Support Vaisala ASCII TSV NLDN data.

### Bug Fixes

- Add fake afos entries for UZ(NT|PA|PN) for REPNT3.
- Allow non-K sites through faked DSM AFOS creator.
- Fix XMPP/Jabber client login/workflow.
- Correct log setup for messages coming from pyIEM and pyWWA.
- Populate all AFOS database fields for RR7 workflow.

## **1.7.0** (6 Jan 2025)

### API Changes

### New Features

### Bug Fixes

- Trim log and emailed message when product is binary (#243).

## **1.6.0** (6 May 2024)

### API Changes

- Change AFOS database save to not include windows carriage return nor the
start of product ``\001`` control character.

### New Features

### Bug Fixes

- Correct `get_example_filepath` logic to assume `pwd` for location of examples.
- Swallow BUFR situation of year==0, which is likely some climatology product?

## **1.5.0** (7 Mar 2024)

### API Changes

### New Features

- [SHEF] Add `pywwa_shef_afos_exclude` setting to allow skipping ill-formed
AFOS identifiers (ie RTPMEG).

### Bug Fixes

- Correct xmpp client resource setting by removing invalid `rstrip()`.
- [SHEF] Ensure looping calls don't quit running and ensure that `save_current`
does not error with GIGO from #215.
- Fix xmpp client responses for ping and version requests #220.

## **1.4.0** (9 Jan 2024)
