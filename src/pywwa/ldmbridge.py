"""A Twisted Python bridge for pqact exec'd processing"""
# stdlib
import datetime
import hashlib
from io import BytesIO

# twisted imports
from twisted.internet import stdio
from twisted.protocols import basic
from twisted.python import log


class LDMProductReceiver(basic.LineReceiver):
    """Our Protocol"""

    reactor = None
    product_start = b"\001"
    product_end = b"\r\r\n\003"

    def __init__(self, dedup=False, isbinary=False):
        """Constructor

        Args:
          dedup (boolean): should we attempt to filter out duplicates
          isbinary (boolean): should we not attempt bytes decoding
            process_data will either return a string and bytes
        """
        self.bytes_received = 0
        self.productBuffer = BytesIO()
        # this puts twisted out of the pure line receiver mode
        self.setRawMode()
        self.cbFunc = self.process_data
        self.cache = {}
        self.isbinary = isbinary
        self.dedup = dedup
        if self.dedup:
            self.cbFunc = self.filter_product

    def clean_cache(self):
        """Cull old cache every 90 seconds"""
        threshold = datetime.datetime.utcnow() - datetime.timedelta(hours=4)
        # loop safety with this
        for digest in list(self.cache):
            if self.cache[digest] < threshold:
                self.cache.pop(digest)
        self.reactor.callLater(90, self.clean_cache)  # @UndefinedVariable

    def filter_product(self, original):
        """Implement Deduplication
         - Attempt to account for all of the wild variations that can happen
         with LDM plexing of NOAAPort, WeatherWire, TOC Socket Feed and
         whatever else may be happening

        1. If the character \x1e happens, we ignore it.  SPC issue here
        2. If we find \x17, this is some weather wire thing, we ignore whatever
           comes after it.
        3. We ignore any extraneous trailing space or line returns
        4. We convert tab characters to blank spaces, as one of NWSTG's systems
           does this already and is a source of duplicates
        5. Ignore first 11 bytes in MD5 computation

        If Okay, we end up calling self.process_data() with clean data

        Args:
          original (str): hopefully gaurenteed to be a string type
        """
        clean = original.replace("\x1e", "").replace("\t", "")
        if clean.find("\x17") > 0:
            # log.msg("control-17 found, truncating...")
            clean = clean[: clean.find("\x17")]
        # log.msg("buffer[:20] is : "+ repr(buf[:20]) )
        # log.msg("buffer[-20:] is : "+ repr(buf[-20:]) )
        lines = clean.split("\015\015\012")
        # Trim trailing empty lines
        while lines and lines[-1].strip() == "":
            lines.pop()
        if not lines:
            log.msg("ERROR: filter_product culled entire product (no data?)")
            return
        lines[1] = lines[1][:3]
        clean = "\015\015\012".join(lines)
        # first 11 characters should not be included in hex, like LDM does
        # hashlib works on bytes
        digest = hashlib.md5(clean[11:].encode("utf-8")).hexdigest()
        # log.msg("Cache size is : "+ str(len(self.cache.keys())) )
        # log.msg("digest is     : "+ str(digest) )
        # log.msg("Product Size  : "+ str(len(product)) )
        # log.msg("len(lines)    : "+ str(len(lines)) )
        if digest in self.cache:
            log.msg("DUP! %s" % (",".join(lines[1:5]),))
        else:
            self.cache[digest] = datetime.datetime.utcnow()
            # log.msg("process_data() called")
            self.process_data(clean + "\015\015\012")

    def rawDataReceived(self, data):
        """callback from twisted when raw data is received

        Args:
          data (str or bytes)
        """
        # write the data to our buffer
        self.bytes_received += self.productBuffer.write(data)

        # see how many products we may have
        tokens = self.productBuffer.getvalue().split(self.product_end)
        # If length tokens is 1, then we did not find the splitter
        # print(("len(tokens) is %s, bytes_received is %s"
        #        ) % (len(tokens), self.bytes_received))
        if len(tokens) == 1:
            return

        # Everything up until the last one can always go...
        for token in tokens[:-1]:
            # print("calling cbFunc(%s)" % (token.decode('utf-8')[:11]))
            if self.isbinary:
                # we send bytes
                self.reactor.callLater(0, self.cbFunc, token)
            else:
                # we send str
                self.reactor.callLater(
                    0, self.cbFunc, token.decode("utf-8", "ignore")
                )
        # We have some cruft left over! be careful here as reassignment was
        # not working as expected, so we do things more cleanly
        self.productBuffer.seek(0)
        self.productBuffer.truncate()
        self.productBuffer.write(tokens[-1])

    def connectionLost(self, reason):
        """Fired when STDIN is closed"""
        raise NotImplementedError

    def process_data(self, data):
        """callback function, either str or bytes depending on isbinary"""
        raise NotImplementedError

    def lineReceived(self, line):
        """needless override to make pylint happy"""
        pass


class LDMProductFactory(stdio.StandardIO):
    """Our Factory"""

    def __init__(self, protocol, reactor=None, **kwargs):
        """constructor with a protocol instance"""
        if reactor is None:
            from twisted.internet import reactor
        protocol.reactor = reactor
        if protocol.dedup:
            reactor.callLater(90, protocol.clean_cache)
        stdio.StandardIO.__init__(self, protocol, reactor=reactor, **kwargs)
