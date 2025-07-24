#!/usr/bin/tclsh

#---------------------------------------------------------------------------
#
# Name:    cmdToList
#
# Purpose: return the output of a command as a list
#
# Method:  set list [cmdToList pgm fname]
#
#          pgm   - command to run
#          fname - name of file to read; if the text is output from
#                  a command, the user can either specify 'fname' as " "
#                  or leave it out altogether
#          nbeg  - number of elements to stip off the beginning of the return list
#          nend  - number of elements to stip off the end of the return list
#
# History: 20030519 - Adapted from Unidata MCGUI procedure of same name
#
#---------------------------------------------------------------------------

proc cmdToList { pgm {fname " "} {nbeg 0} {nend 0} } {

    # Open a command pipeline
    set fhandle [open "|$pgm $fname |& cat" r]

    set readlist {}
    while { [gets $fhandle line] >= 0 } {
      lappend readlist $line
    }
    catch {close $fhandle}

    # Trim 'nbeg' elements off of the beginning of the list
    if { $nbeg > 0 } {
      set readlist [lrange $readlist $nbeg end]
    }

    # Trim 'nend' elements off of the end of the list
    if { $nend > 0 } {
      set readlist [lrange $readlist 0 [expr [llength $readlist] - $nend - 1]]
    }

    # Return the list
    return $readlist

}


#---------------------------------------------------------------------------
#
# Name:    Cpp
#
# Purpose: Parse command line arguments and return defaults if not specified
#
# History: 19970810 - Written for Unidata McIDAS-X GUI
#
#---------------------------------------------------------------------------

proc Cpp { num default } {

    # puts In Cpp..."

    global argv

    set value [lindex $argv $num]

    return [expr {($value != "" && $value != "X" && $value != "x") ? $value : $default}]

}

#---------------------------------------------------------------------------
#
# Name:    openSocket
#
# Purpose: Open a socket on localhost and set to be non-blocking
#
# History: 20151120 - Written for ingest of ASCII NLDN data from Vaisala
#
#---------------------------------------------------------------------------

proc openSocket port {

    # Open socket on port 'port' and set for non-blocking reads
    set soc [socket localhost $port]
    fconfigure $soc -blocking 0

    # Return the handle to the open socket
    return $soc

}

#---------------------------------------------------------------------------
#
# Name:    printList
#
# Purpose: Print out a list item-by-item
#
# History: 20050712 - Written for Unidata Tcl/Tk McIDAS-X GUI
#
#---------------------------------------------------------------------------

proc printList { listing } {

    foreach line $listing {
      puts $line
    }

}


#---------------------------------------------------------------------------
#
# Name:    readSocket
#
# Purpose: Read a line from a specified channel (open elsewhere and
#          non-blocking
#
# Input data format:
#
# Example ASCII NLDN record (NB: values are tab separated):
#0	2015	11	17	21	56	47	792557186	30.1957	-94.1421	-11	0	7	11	0.00	0.20	0.10	0.30	2.0	22.2	-5.7	0	1	0	1
#
# Vaisala ASCII NLDN fields (one-based)
#  1 - UALF version, 0 to 1
#  2 - Year, 1970 to 2032
#  3 - Month, with January as 1 and December as 12
#  4 - Day of the month, 1 to 31
#  5 - Hour, 0 to 23
#  6 - Minute, 0 to 59
#  7 - Second, 0 to 60
#  8 - Nanosecond, 0 to 999999999
#  9 - Latitude of the flash location in decimal degrees, to 4 decimal places, -90.0 to 90.0
# 10 - Longitude of the flash location in decimal degrees, to 4 decimal places, -180.0 to 180.0
# 11 - Estimated peak current in kiloAmperes, -9999 to 9999
# 12 - Multiplicity for flash data (1 to 99) or 0 for strokes
# 13 - Number of sensors participating in the solution, 2 to 99
# 14 - Degrees of freedom when optimizing location, 0 to 99
#
#     The next 3 parameters specify an error ellipse measuring a 50th percentile confidence region around
#     the given latitude/longitude location. This field represents the ellipse angle as a clockwise bearing
#     from 0 degrees North, 0 to 180.0 degrees.
#
# 15 - Ellipse angle as a clockwise bearing from 0 degrees north, 0 to 180.0 degrees
# 16 - Ellipse semi-major axis length in kilometers, 0 to 50.0km
# 17 - Ellipse semi-minor axis length in kilometers, 0 to 50.0km
# 18 - Chi-squared value from location optimization, 0 to 999.99
# 19 - Risetime of the waveform in microseconds, 0 to 99.9
# 20 - Peak-to-zero time of the waveform in microseconds, 0 to 999.9
# 21 - Maximum rate-of-rise of the waveform in kA/usec, 0 to 999.9
# 22 - Cloud indicator, 1 if Cloud-to-cloud discharge, 0 for Cloud-to-ground
# 23 - Angle indicator, 1 if sensor angle data used to compute position, 0 otherwise
# 24 - Signal indicator, 1 if sensor signal data used to compute position, 0 otherwise
# 25 - Timing indicator, 1 if sensor timing data used to compute position, 0 otherwise
#
# Output data format (lacking header):
#
# Binary format and append the new flash record to NldnFlashRecords
#
# NLDN flash record format:
#
# int[4]   seconds since 1970
# int[4]   nanoseconds since tsec (appears be milliseconds)
# int[4]   latitude [deg] * 1000
# int[4]   longitude [deg] * 1000 (west negative convention)
# short[2] null padding
# short[2] signal strength * 10 (150 NLDN measures ~= 30 kAmps)
# short[2] null padding
# char[1]  multiplicity (#strokes per flash)
# char[1]  cloud indicator (1 - cloud-cloud; 0 - cloud-ground)  **NEW**
# char[1]  semi-major axis
# char[1]  eccentricity
# char[1]  ellipse angle
# char[1]  chi-square
#
# History: 20151120 - Written for ingest of ASCII NLDN data from Vaisala
#
#---------------------------------------------------------------------------

proc readSocket socketID {

    global nFlash
    global nTsecs
    global mWait
    global wIter
    global wMsecs
    global NldnFlashRecords

    set bmsecs [clock milliseconds]
    while { [gets $socketID line] > 0 } {

      incr nFlash
      set nldnlist [split $line \t]

      # NB: Tcl list indexing is 0-based
      set ccyy  [lindex $nldnlist  1]
      set mon   [lindex $nldnlist  2]
      set day   [lindex $nldnlist  3]
      set hour  [lindex $nldnlist  4]
      set min   [lindex $nldnlist  5]
      set sec   [lindex $nldnlist  6]
      set nano  [lindex $nldnlist  7]
      set lat   [lindex $nldnlist  8]
      set lon   [lindex $nldnlist  9]
      set sgnl  [lindex $nldnlist 10]
      set mult  [lindex $nldnlist 11]
      set elps  [lindex $nldnlist 14]
      set semiM [lindex $nldnlist 15]
      set semim [lindex $nldnlist 16]
      set chi2  [lindex $nldnlist 17]
      set icld  [lindex $nldnlist 21]
   
      # Integral and scaled (if needed) versions of select parameters
      set usec   [expr $nano / 1000]
      set ilat   [expr int($lat * 1000)]
      set ilon   [expr int($lon * 1000)]
      set isgnl  [expr $sgnl * 10]
      set isemiM [expr int($semiM)]
      set iecc   [expr int(10 * sqrt(1 - (pow(${semim},2) / pow(${semiM},2))))]
      set ielps  [expr int($elps)]
      set ichi2  [expr int($chi2)]

      # Number of seconds since Jan 1, 1970
      set secs [clock scan "$ccyy $mon $day $hour $min $sec" -format {%Y %m %d %H %M %S}]

      # Debug output
      #set fmt "%4d %d %d %6d %7d %5d %4d %3d %3d %3d"
      #puts [format $fmt $nFlash $secs $usec $ilat $ilon $isgnl $mult $ielps $isemiM $ichi2]
      #puts "Appending binary flash record to NldnFlashRecords, nFlash = $nFlash"
      if { $icld == 0 } {
        # 20151123.2305 UTC - Don't include cloud-to-cloud reports
        append NldnFlashRecords [binary format IIIIx2Sx2cccccc $secs $usec $ilat $ilon $isgnl $mult $icld $isemiM $iecc $ielps $ichi2]
      }

      # Increment time counter
      set emsecs [clock milliseconds]
      incr nTsecs [expr $emsecs - $bmsecs]
      set bmsecs $emsecs

      # Write output if elapsed time (nTsecs) >= specified threshold (wIter)
      if { $nTsecs >= $wIter } {
        writeNLDNProduct
      }

    }

    # Increment time counter
    set emsecs [clock milliseconds]
    incr nTsecs [expr $emsecs - $bmsecs]

    # Check to see if need to write output file
    if { $nTsecs >= $wIter } {
      writeNLDNProduct
    }

    set wMsecs [expr $wIter - $nTsecs]
    if { $wMsecs > $mWait } {
      set wMsecs $mWait
    }

    # Done

    return

}


#---------------------------------------------------------------------------
#
# Name:    writeNLDNProduct
#
# Purpose: Write binary-formatted legacy LDM/IDD products to disk
#
# Output data format:
#
#         NLDN flash record header:
#
#         char(4)    'NLDN'
#         int(4)     # of 28 byte chunks needed for header (always 3)
#         int(4)     # flash records in product (>= 0)
#         char(16)   'GDS SUNY@ALBANY '
#         char(28)   'Wed Dec 29 23:07:00 2010    '
#         char(28)   'striker2.atmos.albany.edu   '
#
# History: 20151120 - Written for ingest of ASCII NLDN data from Vaisala
#
#---------------------------------------------------------------------------

proc writeNLDNProduct {} {

    global dStream
    global host
    global logFile
    global nFlash
    global nTsecs
    global NldnFlashRecords
    global outdir

    set nheadr 3
    set iden   "GDS SUNY@ALBANY "
    set secs   [clock seconds]
    set date   [clock format $secs -format {%a %b %e %H:%M:%S %Y %Z}]
    set fname  [clock format $secs -format {%Y%j%H%M%S}]
    set oname  "${outdir}/$fname"

    # Open the ProductID file for writing in binary mode
    set ohandle [open $oname w]
    fconfigure $ohandle -translation binary

    # Create and write NLDN flash record header
    #set NldnFlashHeader [binary format a4IIa16a28a28 NLDN $nheadr [expr $nheadr + $nFlash] $iden $date $host]
    set NldnFlashHeader [binary format a4IIa16a28a28 NLDN $nheadr $nFlash $iden $date $host]
    puts -nonewline $ohandle $NldnFlashHeader

    # Write the flash records block _if_ there are flashes to write
    if { $nFlash > 0 } {
      puts -nonewline $ohandle $NldnFlashRecords
    }

    # Done writing flash record
    close $ohandle

    # Insert the newly created NLDN product into the LDM product queue
    set listing [cmdToList pqinsert.sh "$oname $dStream $logFile"]
    #printList $listing

    # Reset counters
    set nFlash 0
    set nTsecs 0
    set NldnFlashRecords {}

    # Done
    return

}

#
# readNLDNSocket.tcl
#
# Purpose: Does the following:
#
#          - reads ASCII NLDN records from a socket (12345) connected
#            to Vaisala's NLDN distribution system
#          - extracts variables needed to create legacy LDM/IDD NLDN
#            products
#          - creates legacy LDM/IDD NLDN products once per minute
#          - writes the legacy LDM/IDD NLDN products to disk
#          - inserts the legacy LDM/IDD NLDN products in the queue
#            of a running LDM
#          - logs the insertion of the legacy LDM/IDD NLDN products
#            in the LDM queue
#
# Documentation:
#
# History:  20151120 - created for LDM/IDD distribution of NLDN products
#

# Modify PATH so that things like top(1) will work
set env(PATH) /bin:/usr/bin:/usr/local/bin:/free/unidata/ldm/bin:/free/unidata/ldm/util:$env(PATH)

# Get command line parameters
set nMin    [Cpp 0 "1"]                                  ; # product create frequency [min]
set dStream [Cpp 1 "LIGHTNING"]                          ; # LDM/IDD datastream
set logFile [Cpp 2 "/free/unidata/logs/nldn_insert.log"] ; # log file

# Find out other invocations of uptime.tcl are running and exit if there are
set myPid [pid]
set date [clock format [clock seconds] -format {%a %b %e %H:%M:%S %Y %Z}]
set listing [cmdToList ps "-eaf | grep readNLDNSocket.tcl | grep -v vi | grep -v /bin/sh | grep -v grep"]
if { [llength $listing] > 1 } {
  # Exit if script is already running
  puts "${date}: readNLDNSocket.tcl is already running, exiting."
  exit
} else {
  puts "${date}: readNLDNSocket.tcl start"
}

# Initialize variables
set nFlash  0
set nTsecs  0
set wIter   [expr $nMin * 60 * 1000]
set mWait   100
set wMsecs  [expr $mWait - $nTsecs]
if [catch {set hostname [exec hostname 2>/dev/null]}] {
  set hostname "striker.atmos.albany.edu"
}
set host [format %28s $hostname]
if { [info exists env(LDMHOME)] } {
  set ldmhome $env(LDMHOME)
} else {
  set ldmhome /free/unidata/runtime
}
set outdir "${ldmhome}/var/data/nldn/nldnldmdata/raw"

# Open socket for reading ASCII NLDN records
set socketID [openSocket 12345]
#fileevent $socketID readable "readSocket $socketID"

# Main event loop
while { 1 } {

  # Wait for up to 0.1 seconds
  after $wMsecs
  incr nTsecs $wMsecs

  # Read the socket, format output and insert into LDM queue
  readSocket $socketID

}

# Shouldn't get here!
exit
