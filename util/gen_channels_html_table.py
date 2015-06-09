"""Utility script to generate the HTML used for IEMBot Channel Documentation
"""
from pyiem.reference import prodDefinitions
from pyiem.nws.ugc import UGC
from pyiem.nws.nwsli import NWSLI
from pyiem.nws.products.vtec import parser as vtec_parser
from pyiem.nws.products import parser as productparser
import psycopg2.extras
import re

ugc_dict = {}
nwsli_dict = {}

C1 = "&lt;wfo&gt;"
C2 = "&lt;vtec_phenomena&gt;.&lt;vtec_significance&gt;"
C3 = "&lt;afos_pil&gt;"
C4 = "&lt;vtec_phenomena&gt;.&lt;vtec_significance&gt;.&lt;wfo&gt;"
C5 = "&lt;vtec_phenomena&gt;.&lt;vtec_significance&gt;.&lt;ugc&gt;"
C6 = "&lt;ugc&gt;"
D = {
     '10-313': 'http://www.nws.noaa.gov/directives/sym/pd01003013curr.pdf',
     '10-314': 'http://www.nws.noaa.gov/directives/sym/pd01003014curr.pdf',
     '10-315': 'http://www.nws.noaa.gov/directives/sym/pd01003015curr.pdf',
     '10-320': "http://www.nws.noaa.gov/directives/sym/pd01003020curr.pdf",
     '10-401': 'http://www.nws.noaa.gov/directives/sym/pd01004001curr.pdf',
     '10-515': 'http://www.nws.noaa.gov/directives/sym/pd01005015curr.pdf',
     '10-601': 'http://www.nws.noaa.gov/directives/sym/pd01006001curr.pdf',
     '10-923': 'http://www.nws.noaa.gov/directives/sym/pd01009023curr.pdf',
    }
# Our dictionary of products!
VTEC_PRODUCTS = [
    dict(afos='CFW', directive='10-320', channels=[C1, C2, C3, C4, C5, C6]),
    dict(afos='EWW', directive='10-601', channels=[C1, C2, C3, C4, C5, C6]),
    dict(afos='FFA', directive='10-923', channels=[C1, C2, C3, C4, C5, C6]),
    dict(afos='FFS', directive='10-923', channels=[C1, C2, C3, C4, C5, C6]),
    dict(afos='FFW', directive='10-923', channels=[C1, C2, C3, C4, C5, C6]),
    dict(afos='FLS', directive='10-923', channels=[C1, C2, C3, C4, C5, C6]),
    dict(afos='FLW', directive='10-923', channels=[C1, C2, C3, C4, C5, C6]),
    dict(afos='MWW', directive='10-315', channels=[C2, C3, C4, C5, C6],
         notes=("This product does not get routed to the "
                "<span class=\"badge\">&lt;wfo&gt;</span> "
                "channel.  This is because the product is very frequently "
                "issued for offices with marine zones.")),
    dict(afos='NPW', directive='10-515', channels=[C1, C2, C3, C4, C5, C6]),
    dict(afos='RFW', directive='10-401', channels=[C1, C2, C3, C4, C5, C6]),
    dict(afos='SMW', directive='10-313', channels=[C1, C2, C3, C4, C5, C6]),
    dict(afos='', directive='', channels=[]),
    dict(afos='', directive='', channels=[]),
    dict(afos='', directive='', channels=[]),
    dict(afos='', directive='', channels=[]),
    dict(afos='', directive='', channels=[]),
    dict(afos='', directive='', channels=[]),
    dict(afos='', directive='', channels=[]),
                 ]
GEN_PRODUCTS = [
    dict(afos='HLS', directive='10-601', channels=[C3]),
    dict(afos='MWS', directive='10-314', channels=[C3]),
                ]


def get_data(afos):
    """Return the text data for a given afos identifier"""
    return open(("/home/akrherz/projects/pyIEM/data/channel_examples/%s.txt"
                 ) % (afos,)).read()


def load_dicts():
    """Load up the directionaries"""
    pgconn = psycopg2.connect(database='postgis', host='iemdb', user='nobody')
    cursor = pgconn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    sql = """SELECT name, ugc, wfo from ugcs WHERE
        name IS NOT Null and end_ts is null"""
    cursor.execute(sql)
    for row in cursor:
        nm = (row["name"]).replace("\x92", " ").replace("\xc2", " ")
        wfos = re.findall(r'([A-Z][A-Z][A-Z])', row['wfo'])
        ugc_dict[row['ugc']] = UGC(row['ugc'][:2], row['ugc'][2],
                                   row['ugc'][3:],
                                   name=nm,
                                   wfos=wfos)

    sql = """SELECT nwsli,
     river_name || ' ' || proximity || ' ' || name || ' ['||state||']' as rname
     from hvtec_nwsli"""
    cursor.execute(sql)
    for row in cursor:
        nm = row['rname'].replace("&", " and ")
        nwsli_dict[row['nwsli']] = NWSLI(row['nwsli'], name=nm)


def do_generic():
    print("""
    <h3>NWS Local Office Products</h3>
    <table class="table table-bordered">
    <thead>
    <tr><td></td><th>AFOS PIL + Product Name</th><th>Directive</th>
    <th>Channel Templates Used</th></tr>
    </thead>
    """)
    for entry in GEN_PRODUCTS:
        afos = entry['afos']
        if afos == '':
            continue
        v = productparser(get_data(afos), ugc_provider=ugc_dict,
                          nwsli_provider=nwsli_dict)
        j = v.get_jabbers("http://mesonet.agron.iastate.edu/p.php?",
                          "http://mesonet.agron.iastate.edu/p.php?")
        jmsg = ""
        tweet = ""
        channels = []
        for (_, html, xtra) in j:
            tweet += xtra['twitter'] + "<br />"
            jmsg += html
            for channel in xtra['channels'].split(","):
                if channel not in channels:
                    channels.append(channel)
        channels.sort()
        print("""<tr><td>
        <a id="%s_btn" class="btn btn-small" role="button"
        href="javascript: revdiv('%s');"><i class="glyphicon glyphicon-plus"></i></a>
        </td><td>%s</td><td><a href="%s">%s</a></td><td>%s</td></tr>
        <tr><td colspan="4"><div id="%s" style="display:none;">
        <dl class="dl-horizontal">
        %s
        <dt>Channels for Product Example:</dt><dd>%s</dd>
        <dt>XMPP Chatroom Example:</dt><dd>%s</dd>
        <dt>Twitter Example:</dt><dd>%s</dd>
        </dl>
        </div>
        </td>
        </tr>
        """ % (afos, afos, prodDefinitions.get(afos, afos),
               D[entry['directive']], entry['directive'],
               " ".join(["<span class=\"badge\">%s</span>" % (s,) for s in entry['channels']]),
               afos,
               '<dt>Notes</dt><dd>%s</dd>' % (entry.get('notes'),) if 'notes' in entry else '',
               " ".join(["<span class=\"badge\">%s</span>" % (s,) for s in channels]),
               jmsg, tweet))

    print("""</table>""")


def do_vtec():
    print("""
    <h3>NWS Products with P-VTEC and/or H-VTEC Included</h3>
    <table class="table table-bordered">
    <thead>
    <tr><td></td><th>AFOS PIL + Product Name</th><th>Directive</th>
    <th>Channel Templates Used</th></tr>
    </thead>
    """)
    for entry in VTEC_PRODUCTS:
        afos = entry['afos']
        if afos == '':
            continue
        v = vtec_parser(get_data(afos), ugc_provider=ugc_dict,
                        nwsli_provider=nwsli_dict)
        j = v.get_jabbers("http://mesonet.agron.iastate.edu/vtec/",
                          "http://mesonet.agron.iastate.edu/vtec/")
        jmsg = ""
        tweet = ""
        channels = []
        for (_, html, xtra) in j:
            tweet += xtra['twitter'] + "<br />"
            jmsg += html
            for channel in xtra['channels'].split(","):
                if channel not in channels:
                    channels.append(channel)
        channels.sort()
        print("""<tr><td>
        <a id="%s_btn" class="btn btn-small" role="button"
        href="javascript: revdiv('%s');"><i class="glyphicon glyphicon-plus"></i></a>
        </td><td>%s</td><td><a href="%s">%s</a></td><td>%s</td></tr>
        <tr><td colspan="4"><div id="%s" style="display:none;">
        <dl class="dl-horizontal">
        %s
        <dt>Channels for Product Example:</dt><dd>%s</dd>
        <dt>XMPP Chatroom Example:</dt><dd>%s</dd>
        <dt>Twitter Example:</dt><dd>%s</dd>
        </dl>
        </div>
        </td>
        </tr>
        """ % (afos, afos, prodDefinitions.get(afos, afos),
               D[entry['directive']], entry['directive'],
               " ".join(["<span class=\"badge\">%s</span>" % (s,) for s in entry['channels']]),
               afos,
               '<dt>Notes</dt><dd>%s</dd>' % (entry.get('notes'),) if 'notes' in entry else '',
               " ".join(["<span class=\"badge\">%s</span>" % (s,) for s in channels]),
               jmsg, tweet))

    print("""</table>""")


def main():
    """Do Something Fun"""
    load_dicts()
    print("""
    <style>
    .badge {
        background-color: #EEEEEE;
        color: #000;
    }
    .dl-horizontal dt {
        white-space: normal;
        padding-bottom: 10px;
    }
    </style>
    """)
    do_vtec()
    do_generic()

    print("""
    <script>
    function revdiv(myid){
      var $a = $('#'+myid);
      if ($a.css('display') == 'block'){
        $a.css('display', 'none');
        $("#"+myid+"_btn").html('<i class="glyphicon glyphicon-plus"></i>');
      } else {
        $a.css('display', 'block');
        $("#"+myid+"_btn").html('<i class="glyphicon glyphicon-minus"></i>');
      }
    }
    </script>
    """)

if __name__ == '__main__':
    # Go Main Go
    main()
