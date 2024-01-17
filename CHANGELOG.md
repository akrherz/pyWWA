<!-- markdownlint-configure-file {"MD024": { "siblings_only": true } } -->
# Changelog

All notable changes to this library are documented in this file.

## Unreleased version

### API Changes

### New Features

- [SHEF] Add `pywwa_shef_afos_exclude` setting to allow skipping ill-formed
AFOS identifiers (ie RTPMEG).

### Bug Fixes

- [SHEF] Ensure looping calls don't quit running and ensure that `save_current`
does not error with GIGO from #215.
- Fix xmpp client responses for ping and version requests #220.

## **1.4.0** (9 Jan 2024)
