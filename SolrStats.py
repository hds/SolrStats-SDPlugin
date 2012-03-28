import sys
import re
import urllib2, httplib
import logging
from elementtree.ElementTree import parse

from xml.parsers.expat import ExpatError
#from xml.dom.minidom import parse

class SolrStats:
    def __init__(self, agent_config, checks_logger, raw_config):
        self.agent_config = agent_config
        self.checks_logger = checks_logger
        self.raw_config = raw_config
        self.main_config = raw_config['Main']
        
        self.whitelist = None
        self.blacklist = None
        self.skip_inactive = False
        
        if 'solr_stats_whitelist' in self.main_config:
            self.whitelist = self.main_config['solr_stats_whitelist']
        if 'solr_stats_blacklist' in self.main_config:
            self.blacklist = self.main_config['solr_stats_blacklist']
        if 'solr_stats_skip_inactive' in self.main_config:
            skip_val = self.main_config['solr_stats_skip_inactive'].lower()
            if skip_val == 'yes' or skip_val == 'true' or skip_val == '1':
                self.skip_inactive = True

    def fetch_stats(self, url):
        #url = 'http://panthroint.titan.net:8988/admin/stats.jsp'

        response = urllib2.urlopen(url)
        
        return response

    def parse_stats(self, xmlfile):
        data = { }
        stats = parse(xmlfile).getroot()

        schema = re.sub(r'^\s+|\s+$', '', stats.find('schema').text)
        schema = re.sub(r'\s+', '_', schema)

        for entry in stats.findall('solr-info/CORE/entry'):
            for stat in entry.findall('stats/stat'):
                if stat.attrib['name'] in ['numDocs', 'maxDoc']:
                    attr = stat.attrib['name']
                    val = re.sub(r'^\s+|\s+$', '', stat.text)
                    data['%s_%s' % (schema, attr)] = val

        for entry in stats.findall('solr-info/QUERYHANDLER/entry'):
            name = re.sub(r'^\s+|\s+$', '', entry.find('name').text)
            name = re.sub(r'^org\.apache\.solr\.handler\.', '', name)
            name = re.sub(r'^/', '', name)
            name = re.sub(r'/', '_', name)
            if name != 'standard':
                continue
            entry_data = { }
            for stat in entry.findall('stats/stat'):
                if stat.attrib['name'] in ['requests', 'errors', 'timeouts', 'avgTimePerRequest', 'avgRequestsPerSecond']:
                    attr = stat.attrib['name']
                    val = re.sub(r'^\s+|\s+$', '', stat.text)
                    entry_data['%s_%s' % (schema, attr)] = val

            data.update(entry_data)

        return data

    def run(self):

        self.mainLogger = logging.getLogger('main')
        if 'solr_stats_urls' not in self.main_config:
            self.mainLogger.error('solr_stats_urls MUST be in the config')
            return False

        urls = self.main_config['solr_stats_urls']
        data = { }

        if not isinstance(urls, list):
            urls = [ urls ]
        for url in urls:
            ## Fetch Solr Stats ##
            try:
                response = self.fetch_stats(url)
            except MissingConfiguration, e:
                return False
            except urllib2.HTTPError, e:
                self.mainLogger.error('Unable to get Solr stats, HTTPError: %s' % (str(e),))
                return False
            except urllib2.URLError, e:
                self.mainLogger.error('Unable to get Solr stats, URLError: %s' % (str(e),))
                return False
            except httplib.HTTPException, e:
                self.mainLogger.error('Unable to get Solr stats, HTTPException: %s' % (str(e),))
                return False
            except Exception, e:
                import traceback
                self.mainLogger.error('Unable to get Solr stats, Exception: %s' % (traceback.format_exc(),))
                return False


            ## Process stats ##
            try:
                new_data = self.parse_stats(response)
            except ExpatError, e:
                self.mainLogger.error('Unable to parse Solr stats, ExpatError: %s' % (str(e),))
                return False
            except Exception, e:
                import traceback
                self.mainLogger.error('Unable to parse Solr stats, Exception: %s' % (traceback.format_exc(),))
                return False
            data.update(new_data)


        return data

class MissingConfiguration(Exception):
    pass

if __name__ == '__main__':
    mainLogger = logging.getLogger('main')
    mainLogger.addHandler(logging.StreamHandler(sys.stderr))
    # fp, total, books, mags
    solr_stats = SolrStats(None, None, {'Main': {'solr_stats_urls':['http://panthroint.titan.net:8992/admin/stats.jsp', 'http://panthroint.titan.net:8987/admin/stats.jsp', 'http://panthroint.titan.net:8985/admin/stats.jsp', 'http://panthroint.titan.net:8984/admin/stats.jsp', 'http://panthroint.titan.net:8994/admin/stats.jsp']}})
    data = solr_stats.run()
    print data
