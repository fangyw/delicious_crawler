"""
    Unofficial Python API for retrieving data from Delicious.com.

    This module provides the following features plus some more:

    * retrieving a URL's full public bookmarking history including
        * users who bookmarked the URL including tags used for such bookmarks
        * top tags (up to a maximum of 10) including tag count
        * title as stored on Delicious.com
        * total number of bookmarks/users for this URL at Delicious.com
    * retrieving a user's full bookmark collection, including any private bookmarks
      if you know the corresponding password
    * retrieving a user's full public tagging vocabulary, i.e. tags and tag counts
    * retrieving a user's network information (network members and network fans)
    * HTTP proxy support
    * updated to support Delicious.com "version 2" (mini-relaunch as of August 2008)

    The official Delicious.com API and the JSON/RSS feeds do not provide all
    the functionality mentioned above, and in such cases this module will query
    the Delicious.com *website* directly and extract the required information
    by parsing the HTML code of the resulting Web pages (a kind of poor man's
    web mining). The module is able to detect IP throttling, which is employed
    by Delicious.com to temporarily block abusive HTTP request behavior, and
    will raise a custom Python error to indicate that. Please be a nice netizen
    and do not stress the Delicious.com service more than necessary.

    It is strongly advised that you read the Delicious.com Terms of Use
    before using this Python module. In particular, read section 5
    'Intellectual Property'.

    The code is licensed to you under version 2 of the GNU General Public
    License.

    More information about this module can be found at
    http://www.michael-noll.com/wiki/Del.icio.us_Python_API

    Changelog is available at
    http://code.michael-noll.com/?p=deliciousapi;a=log

    Copyright 2006-2010 Michael G. Noll <http://www.michael-noll.com/>

"""

__author__ = "Michael G. Noll"
__copyright__ = "(c) 2006-2010 Michael G. Noll"
__description__ = "Unofficial Python API for retrieving data from Delicious.com"
__email__ = "coding[AT]michael-REMOVEME-noll[DOT]com"
__license__ = "GPLv2"
__maintainer__ = "Michael G. Noll"
__status__ = "Development"
__url__ = "http://www.michael-noll.com/"
__version__ = "1.6.7"

import cgi
import datetime
import hashlib
from operator import itemgetter
import socket
import time
import urllib2
import urllib
from logger import get_logger

try:
    from lxml import etree 
except:
    print "ERROR: could not import lxml module"
    print
    print"You can install lxml via pip install lxml" 
    raise

try:
    import simplejson
except:
    print "ERROR: could not import simplejson module"
    print
    print "Since version 1.5.0, DeliciousAPI requires the simplejson module."
    print "You can download simplejson from the Python Cheese Shop at"
    print "http://pypi.python.org/pypi/simplejson"
    print
    raise

log = get_logger('log.txt')

class DeliciousUser(object):
    """This class wraps all available information about a user into one object.

    Variables:
        bookmarks:
            A list of (url, tags, title, comment) tuples representing
            a user's bookmark collection.

            url is a 'unicode'
            tags is a 'list' of 'unicode' ([] if no tags)
            title is a 'unicode'
            comment is a 'unicode' (u"" if no comment)
            
        tags (read-only property):
            A list of (tag, tag_count) tuples, aggregated over all a user's
            retrieved bookmarks. The tags represent a user's tagging vocabulary.

        username:
            The Delicious.com account name of the user.

    """

    def __init__(self, username, bookmarks=None):
        assert username
        self.username = username
        self.bookmarks = bookmarks or []

    def __str__(self):
        total_tag_count = 0
        total_tags = set()
        for url, tags, title, comment in self.bookmarks:
            if tags:
                total_tag_count += len(tags)
            for tag in tags:
                total_tags.add(tag)
        return "[%s] %d bookmarks, %d tags (%d unique)" % \
                    (self.username, len(self.bookmarks), total_tag_count, len(total_tags))

    def __repr__(self):
        return self.username

    def get_tags(self):
        """Returns a dictionary mapping tags to their tag count.

        For example, if the tag count of tag 'foo' is 23, then
        23 bookmarks were annotated with 'foo'. A different way
        to put it is that 23 users used the tag 'foo' when
        bookmarking the URL.

        """
        total_tags = {}
        for url, tags, title, comment in self.bookmarks:
            for tag in tags:
                total_tags[tag] = total_tags.get(tag, 0) + 1
        return total_tags
    tags = property(fget=get_tags, doc="Returns a dictionary mapping tags to their tag count")


class DeliciousURL(object):
    """This class wraps all available information about a web document into one object.

    Variables:
        bookmarks:
            A list of (user, tags, comment, timestamp) tuples, representing a
            document's bookmark history. Generally, this variable is populated
            via get_url(), so the number of bookmarks available in this variable
            depends on the parameters of get_url(). See get_url() for more
            information.

            user is a 'unicode'
            tags is a 'list' of 'unicode's ([] if no tags)
            comment is a 'unicode' (u"" if no comment)
            timestamp is a 'datetime.datetime' (granularity: creation *day*,
                i.e. the day but not the time of day)

        tags (read-only property):
            A list of (tag, tag_count) tuples, aggregated over all a document's
            retrieved bookmarks.

        top_tags:
            A list of (tag, tag_count) tuples, representing a document's so-called
            "top tags", i.e. the up to 10 most popular tags for this document.

        url:
            The URL of the document.

        hash (read-only property):
            The MD5 hash of the URL.

        title:
            The document's title.

        total_bookmarks:
            The number of total bookmarks (posts) of the document.
            Note that the value of total_bookmarks can be greater than the
            length of "bookmarks" depending on how much (detailed) bookmark
            data could be retrieved from Delicious.com.

            Here's some more background information:
            The value of total_bookmarks is the "real" number of bookmarks of
            URL "url" stored at Delicious.com as reported by Delicious.com
            itself (so it's the "ground truth"). On the other hand, the length
            of "bookmarks" depends on iteratively scraped bookmarking data.
            Since scraping Delicous.com's Web pages has its limits in practice,
            this means that DeliciousAPI could most likely not retrieve all
            available bookmarks. In such a case, the value reported by
            total_bookmarks is greater than the length of "bookmarks".

    """

    def __init__(self, url, top_tags=None, bookmarks=None, title=u"", total_bookmarks=0):
        assert url
        self.url = url
        self.top_tags = top_tags or []
        self.bookmarks = bookmarks or []
        self.title = title
        self.total_bookmarks = total_bookmarks

    def __str__(self):
        total_tag_count = 0
        total_tags = set()
        for user, tags, comment in self.bookmarks:
            if tags:
                total_tag_count += len(tags)
            for tag in tags:
                total_tags.add(tag)
        return "[%s] %d total bookmarks (= users), %d tags (%d unique), %d out of 10 max 'top' tags" % \
                    (self.url, self.total_bookmarks, total_tag_count, \
                    len(total_tags), len(self.top_tags))

    def __repr__(self):
        return self.url

    def get_tags(self):
        """Returns a dictionary mapping tags to their tag count.

        For example, if the tag count of tag 'foo' is 23, then
        23 bookmarks were annotated with 'foo'. A different way
        to put it is that 23 users used the tag 'foo' when
        bookmarking the URL.

        @return: Dictionary mapping tags to their tag count.

        """
        total_tags = {}
        for user, tags, comment in self.bookmarks:
            for tag in tags:
                total_tags[tag] = total_tags.get(tag, 0) + 1
        return total_tags
    tags = property(fget=get_tags, doc="Returns a dictionary mapping tags to their tag count")

    def get_hash(self):
        m = hashlib.md5()
        m.update(self.url)
        return m.hexdigest()
    hash = property(fget=get_hash, doc="Returns the MD5 hash of the URL of this document")


class DeliciousAPI(object):
    """
    This class provides a custom, unofficial API to the Delicious.com service.

    Instead of using just the functionality provided by the official
    Delicious.com API (which has limited features), this class retrieves
    information from the Delicious.com website directly and extracts data from
    the Web pages.

    Note that Delicious.com will block clients with too many queries in a
    certain time frame (similar to their API throttling). So be a nice citizen
    and don't stress their website.

    """

    def __init__(self,
                    http_proxy="",
                    tries=3,
                    wait_seconds=3,
                    user_agent="Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)",
                    timeout=30
        ):
        """Set up the API module.

        @param http_proxy: Optional, default: "".
            Use an HTTP proxy for HTTP connections. Proxy support for
            HTTPS is not available yet.
            Format: "hostname:port" (e.g., "localhost:8080")
        @type http_proxy: str

        @param tries: Optional, default: 3.
            Try the specified number of times when downloading a monitored
            document fails. tries must be >= 1. See also wait_seconds.
        @type tries: int

        @param wait_seconds: Optional, default: 3.
            Wait the specified number of seconds before re-trying to
            download a monitored document. wait_seconds must be >= 0.
            See also tries.
        @type wait_seconds: int

        @param user_agent: Optional, default: "DeliciousAPI/<version>
            (+http://www.michael-noll.com/wiki/Del.icio.us_Python_API)".
            The User-Agent HTTP Header to use when querying Delicous.com.
        @type user_agent: str

        @param timeout: Optional, default: 30.
            Set network timeout. timeout must be >= 0.
        @type timeout: int

        """
        assert tries >= 1
        assert wait_seconds >= 0
        assert timeout >= 0
        self.http_proxy = http_proxy
        self.tries = tries
        self.wait_seconds = wait_seconds
        self.user_agent = user_agent
        self.timeout = timeout
        socket.setdefaulttimeout(self.timeout)


    def _query(self, path, host="m.delicious.com", user=None, password=None, use_ssl=False):
        """Queries Delicious.com for information, specified by (query) path.

        @param path: The HTTP query path.
        @type path: str

        @param host: The host to query, default: "delicious.com".
        @type host: str

        @param user: The Delicious.com username if any, default: None.
        @type user: str

        @param password: The Delicious.com password of user, default: None.
        @type password: unicode/str

        @param use_ssl: Whether to use SSL encryption or not, default: False.
        @type use_ssl: bool

        @return: None on errors (i.e. on all HTTP status other than 200).
            On success, returns the content of the HTML response.

        """
        opener = None
        handlers = []

        # add HTTP Basic authentication if available
        if user and password:
            pwd_mgr = urllib2.HTTPPasswordMgrWithDefaultRealm()
            pwd_mgr.add_password(None, host, user, password)
            basic_auth_handler = urllib2.HTTPBasicAuthHandler(pwd_mgr)
            handlers.append(basic_auth_handler)

        # add proxy support if requested
        if self.http_proxy:
            proxy_handler = urllib2.ProxyHandler({'http': 'http://%s' % self.http_proxy})
            handlers.append(proxy_handler)

        if handlers:
            opener = urllib2.build_opener(*handlers)
        else:
            opener = urllib2.build_opener()
        opener.addheaders = [('User-agent', self.user_agent)]

        data = None
        tries = self.tries

        if use_ssl:
            protocol = "https"
        else:
            protocol = "http"
        url = "%s://%s%s" % (protocol, host, path)

        while tries > 0:
            try:
                log.debug('url is %s' % url)
                f = opener.open(url)
                data = f.read()
                f.close()
                break
            except urllib2.HTTPError, e:
                if e.code == 301:
                    raise DeliciousMovedPermanentlyWarning, "Delicious.com status %s - url moved permanently" % e.code
                if e.code == 302:
                    raise DeliciousMovedTemporarilyWarning, "Delicious.com status %s - url moved temporarily" % e.code
                elif e.code == 401:
                    raise DeliciousUnauthorizedError, "Delicious.com error %s - unauthorized (authentication failed?)" % e.code
                elif e.code == 403:
                    raise DeliciousForbiddenError, "Delicious.com error %s - forbidden" % e.code
                elif e.code == 404:
                    raise DeliciousNotFoundError, "Delicious.com error %s - url not found" % e.code
                elif e.code == 500:
                    raise Delicious500Error, "Delicious.com error %s - server problem" % e.code
                elif e.code == 503 or e.code == 999:
                    raise DeliciousThrottleError, "Delicious.com error %s - unable to process request (your IP address has been throttled/blocked)" % e.code
                else:
                    raise DeliciousUnknownError, "Delicious.com error %s - unknown error" % e.code
                break
            except urllib2.URLError, e:
                time.sleep(self.wait_seconds)
            except socket.error:
                # sometimes we get a "Connection Refused" error
                # wait a bit and then try again
                time.sleep(self.wait_seconds)
            #finally:
            #    f.close()
            tries -= 1
        return data

    def get_tags_by_user(user):
        """
        return the user's tags in list
        @param user:the username
        """
        path = "v2/json/tags/%s" %user
        data = self._query(path,host="feeds.delicious.com")
        urlinfo = simplejson.loads(data)
        if isinstance(urlinfo,dict):
            tags = urlinfo.keys()
        else:
            tags = []
        return tags

    def tags_is_english(tags):
        '''
        return boolen value when the tags are all english,when all in english,return ture,otherwise false.
        @param tags:tag list
        @type tags:list

        '''
        return "".join(("".join(tags)).split()).isalpha()




    def get_url(self, url, max_bookmarks=50, sleep_seconds=1):
        """
        Returns a DeliciousURL instance representing the Delicious.com history of url.

        Generally, this method is what you want for getting title, bookmark, tag,
        and user information about a URL.

        Delicious only returns up to 50 bookmarks per URL. This means that
        we have to do subsequent queries plus parsing if we want to retrieve
        more than 50. Roughly speaking, the processing time of get_url()
        increases linearly with the number of 50-bookmarks-chunks; i.e.
        it will take 10 times longer to retrieve 500 bookmarks than 50.

        @param url: The URL of the web document to be queried for.
        @type url: str

        @param max_bookmarks: Optional, default: 50.
            See the documentation of get_bookmarks() for more information
            as get_url() uses get_bookmarks() to retrieve a url's
            bookmarking history.
        @type max_bookmarks: int

        @param sleep_seconds: Optional, default: 1.
            See the documentation of get_bookmarks() for more information
            as get_url() uses get_bookmarks() to retrieve a url's
            bookmarking history. sleep_seconds must be >= 1 to comply with
            Delicious.com's Terms of Use.
        @type sleep_seconds: int

        @return: DeliciousURL instance representing the Delicious.com history
            of url.

        """
        # we must wait at//ul[@id='bookmarklist']/li least 1 second between subsequent queries to
        # comply with Delicious.com's Terms of Use
        assert sleep_seconds >= 1

        document = DeliciousURL(url)

        m = hashlib.md5()
        m.update(url)
        hash = m.hexdigest()

        path = "/v2/json/urlinfo/%s" % hash
        data = self._query(path, host="feeds.delicious.com")
        if data:
            urlinfo = {}
            try:
                urlinfo = simplejson.loads(data)
                if urlinfo:
                    urlinfo = urlinfo[0]
                else:
                    urlinfo = {}
            except TypeError:
                pass
            try:
                document.title = urlinfo['title'] or u""
            except KeyError:
                pass
            try:
                top_tags = urlinfo['top_tags'] or {}
                if top_tags:
                    document.top_tags = sorted(top_tags.iteritems(), key=itemgetter(1), reverse=True)
                else:
                    document.top_tags = []
            except KeyError:
                pass
            try:
                document.total_bookmarks = int(urlinfo['total_posts'])
            except (KeyError, ValueError):
                pass
            document.bookmarks = self.get_bookmarks(url=url, max_bookmarks=max_bookmarks, sleep_seconds=sleep_seconds)


        return document

    def get_network(self, username):
        """
        Returns the user's list of followees and followers.

        Followees are users in his Delicious "network", i.e. those users whose
        bookmark streams he's subscribed to. Followers are his Delicious.com
        "fans", i.e. those users who have subscribed to the given user's
        bookmark stream).

        Example:

                A -------->   --------> C
                D --------> B --------> E
                F -------->   --------> F

            followers               followees
            of B                    of B

        Arrows from user A to user B denote that A has subscribed to B's
        bookmark stream, i.e. A is "following" or "tracking" B.

        Note that user F is both a followee and a follower of B, i.e. F tracks
        B and vice versa. In Delicious.com terms, F is called a "mutual fan"
        of B.

        Comparing this network concept to information retrieval, one could say
        that followers are incoming links and followees outgoing links of B.

        @param username: Delicous.com username for which network information is
            retrieved.
        @type username: unicode/str

        @return: Tuple of two lists ([<followees>, [<followers>]), where each list
            contains tuples of (username, tracking_since_timestamp).
            If a network is set as private, i.e. hidden from public view,
            (None, None) is returned.
            If a network is public but empty, ([], []) is returned.

        """
        assert username
        followees = followers = None

        # followees (network members)
        path = "/v2/json/networkmembers/%s" % username
        data = None
        try:
            data = self._query(path, host="feeds.delicious.com")
        except DeliciousForbiddenError:
            pass
        if data:
            followees = []

            users = []
            try:
                users = simplejson.loads(data)
            except TypeError:
                pass

            uname = tracking_since = None

            for user in users:
                # followee's username
                try:
                    uname = user['user']
                except KeyError:
                    pass
                # try to convert uname to Unicode
                if uname:
                    try:
                        # we assume UTF-8 encoding
                        uname = uname.decode('utf-8')
                    except UnicodeDecodeError:
                        pass
                # time when the given user started tracking this user
                try:
                    tracking_since = datetime.datetime.strptime(user['dt'], "%Y-%m-%dT%H:%M:%SZ")
                except KeyError:
                    pass
                if uname:
                    followees.append( (uname, tracking_since) )

        # followers (network fans)
        path = "/v2/json/networkfans/%s" % username
        data = None
        try:
            data = self._query(path, host="feeds.delicious.com")
        except DeliciousForbiddenError:
            pass
        if data:
            followers = []

            users = []
            try:
                users = simplejson.loads(data)
            except TypeError:
                pass

            uname = tracking_since = None

            for user in users:
                # fan's username
                try:
                    uname = user['user']
                except KeyError:
                    pass
                # try to convert uname to Unicode
                if uname:
                    try:
                        # we assume UTF-8 encoding
                        uname = uname.decode('utf-8')
                    except UnicodeDecodeError:
                        pass
                # time when fan started tracking the given user
                try:
                    tracking_since = datetime.datetime.strptime(user['dt'], "%Y-%m-%dT%H:%M:%SZ")
                except KeyError:
                    pass
                if uname:
                    followers.append( (uname, tracking_since) )
        return ( followees, followers )

    def get_bookmarks(self, url=None, username=None, max_bookmarks=100, sleep_seconds=1):
        """
        Returns the bookmarks of url or user, respectively.
        we return a iterator if the user's bookmark are large


        @param url: The URL of the web document to be queried for.
            Cannot be used together with 'username'.
        @type url: str

        @param username: The Delicious.com username to be queried for.
            Cannot be used together with 'url'.
        @type username: str

        @param max_bookmarks: Optional, default: 50.
            this return the `max_bookmarks` bookmarks
            @type max_bookmarks: int

        @param sleep_seconds: Optional, default: 1.
                Wait the specified number of seconds between subsequent
                queries in case that there are multiple pages of bookmarks
                for the given url. sleep_seconds must be >= 1 to comply with
                Delicious.com's Terms of Use.
                See also parameter 'max_bookmarks'.
        @type sleep_seconds: int

        @return: Returns the bookmarks of url or user, respectively.
            For urls, it returns a list of (user, tags, comment)
            tuples.
            For users, it returns a list of (url, tags, title, comment) tuples.

        """
        # we must wait at least 1 second between subsequent queries to
        # comply with delicious' Terms of Use
        assert sleep_seconds >= 1

        # url XOR username 
        assert bool(username) is not bool(url)

        # maximum number of urls/posts Delicious.com will display
        # per page on its website
        # maximum number of pages that Delicious.com will display;
        # currently, the maximum number of pages is 20. Delicious.com
        # allows to go beyond page 20 via pagination, but page N (for
        # N > 20) will always display the same content as page 20.
        #max_html_pages = 20  # no more max_html_pages restricted

        path = None
        if url:
            m = hashlib.md5()
            m.update(url)
            hash_value = m.hexdigest()

            # path will change later on if there are multiple pages of boomarks
            # for the given url
            if 0 < max_bookmarks <= 100:
                path ="/v2/json/url/%s?count=%s" % (hash_value,max_bookmarks)
            else:
                path ="/v2/json/url/%s?count=100" % hash_value
        elif username:
            userinfo_path = "/v2/json/userinfo/%s" %(username)
            userinfo_data = self._query(userinfo_path, host="feeds.delicious.com")
            try:
                user_links_number = int(simplejson.loads(userinfo_data)[0].get("n"))
            except:
                raise Exception('no userinfo json found')
            # path will change later on if there are multiple pages of boomarks
            # for the given username
            if 0 < max_bookmarks <= 100:
                path ="/v2/json/%s?count=%s" % (username,max_bookmarks)
            elif user_links_number < 100:
                path ="/v2/json/%s?count=100" % username
            else:
                path = "/%s" % username

        else:
            raise Exception('You must specify either url or user.')

        page_index = 1
        bookmarks = []
        while path:
            if url:
                data = self._query(path, host="feeds.delicious.com")
                bookmarks.extend(self._extract_bookmarks_from_url_feeds(data))
                path = None
            else:
                if 0 < max_bookmarks <= 100:
                        data = self._query(path, host="feeds.delicious.com")
                        bookmarks.extend(self._extract_bookmarks_from_user_feeds(data))
                        path = None
                elif user_links_number < 100:
                        data = self._query(path, host="feeds.delicious.com")
                        bookmarks.extend(self._extract_bookmarks_from_user_feeds(data))
                        path = None
                else:
                    data = self._query(path)
                    path = None
                    if data:
                        # extract bookmarks from current page
                        if url:
                            bookmarks.extend(self._extract_bookmarks_from_url_history(data))
                        else:
                            bookmarks.extend(self._extract_bookmarks_from_user_history(data))

                        # stop scraping if we already have as many bookmarks as we want
                        if (len(bookmarks) >= max_bookmarks) and max_bookmarks != 0:
                            break
                        else:
                            # check if there are multiple pages of bookmarks for this
                            # url on Delicious.com
                            tree = etree.HTML(data)
                            paginations = tree.xpath("//div[@id='pagination']",smart_strings=False)

                            if paginations:
                                # find next path
                                nexts = tree.xpath("//div[@id='pagination']/span/following::a[1]/@href",smart_strings=False)
                                if nexts and (max_bookmarks == 0 or len(bookmarks) < max_bookmarks) and len(bookmarks) > 0:
                                    # e.g. /url/2bb293d594a93e77d45c2caaf120e1b1?show=all&page=2
                                    path = nexts[0]
                                    page_index += 1
                                    # wait one second between queries to be compliant with
                                    # delicious' Terms of Use
                                    time.sleep(sleep_seconds)
        if max_bookmarks > 0:
            return bookmarks[:max_bookmarks]
        else:
            return bookmarks


    def _extract_bookmarks_from_user_feeds(self, data):
        bookmarks = []
        posts = simplejson.loads(data)
        title = comment = u""

        for post in posts:
            url = post['u']
            title = post['d']
            tags_content = post['t']
            if tags_content in ([""],[],""):
                tags = []
            else:
                tags = tags_content
            comment = post['n']
            bookmarks.append((url,tags,title,comment))
        return bookmarks

    def _extract_bookmarks_from_url_feeds(self, data):
        bookmarks = []
        try:
            posts = simplejson.loads(data)
        except:
            pass
        title = comment = u""

        for post in posts:
            user = post['a']
            title = post['d']
            tags = post['t'] or []
            comment = post['n']
            bookmarks.append((user,tags,title,comment))
        return bookmarks 

    ## in a url page,it return only 100 results,so it's faster to get the data from feeds
    #def _extract_bookmarks_from_url_history(self, data):
        #"""
        #Extracts user bookmarks from a URL's history page on Delicious.com.

        #The Python library lxml is used to parse the HTML page.

        #@param data: The HTML source of a URL history Web page on Delicious.com.
        #@type data: str

        #@return: list of user bookmarks of the corresponding URL

        #"""
        #bookmarks = []
        #tree = etree.HTML(data)
        #bookmark_elements = tree.xpath("//ul[@id='bookmarklist']/li")

        #for bookmark_element in bookmark_elements:
            #if len(bookmark_element.xpath(".//div/*")) != 1:
                ## extract comments
                #comment = u""
                #datas = bookmark_element.xpath(".//div[@class='note']/p/text()")
                #if datas:
                    #comment = datas[0]

                ## extract tags
                #user_tags = []
                #taglist = bookmark_element.xpath(".//div[@class='taglist ']/div")
                #if taglist:
                    #for tag in taglist:
                        #tag_content = tag.xpath(".//div[@class='co']/span/text()")[0].encode("utf-8")
                        #user_tags.append(tag_content)

                ## extract user information
                #user = u""
                #datas1 = bookmark_element.xpath(".//div[@class='source_info']/a/text()")
                #if datas1:
                    #user = datas1[0]

                #bookmarks.append( (user, user_tags, comment) )

        #return bookmarks

    def _extract_bookmarks_from_user_history(self, data):
        """
        Extracts a user's bookmarks from his user page on Delicious.com.

        The Python library lxml is used to parse the HTML page.

        @param data: The HTML source of a user page on Delicious.com.
        @type data: str

        @return: list of bookmarks of the corresponding user

        """
        bookmarks = []
        tree = etree.HTML(data)
        link_lists = tree.xpath("//div[@class='link  False']",smart_strings=False)

        for link in link_lists:
            #extract the link's url
            url = urllib.unquote(link.xpath(".//a[@class='title']/@href",smart_strings=False)[0].split('url=')[-1]) 

            #extract the link's tag
            tags = []
            taglist = link.xpath(".//div[@class='tag ']",smart_strings=False)
            for tag in taglist:
                tag_content = tag.xpath(".//div[@class='co']/span/text()",smart_strings=False)[0]
                tags.append(tag_content)

            #extract the link's title
            title = link.xpath(".//a[@class='title']/text()",smart_strings=False)

            #extract the link's comment
            comment = u""
            comment_data = link.xpath(".//div[@class='note']/p/text()",smart_strings=False)
            if comment_data:
                comment = comment_data

            bookmarks.append((url,tags,title,comment))

        return bookmarks


    def get_user(self, username, password=None, max_bookmarks=50, sleep_seconds=1):
        """Retrieves a user's bookmarks from Delicious.com.

        If a correct username AND password are supplied, a user's *full*
        bookmark collection (which also includes private bookmarks) is
        retrieved. Data communication is encrypted using SSL in this case.

        If no password is supplied, only the *public* bookmarks of the user
        are retrieved. Here, the parameter 'max_bookmarks' specifies how
        many public bookmarks will be retrieved (default: 50). Set the
        parameter to 0 to retrieve all public bookmarks.

        This function can be used to backuhttp://pypi.python.org/pypi/p all of a user's bookmarks if
        called with a username and password.

        @param username: The Delicious.com username.
        @type username: str

        @param password: Optional, default: None.
            The user's Delicious.com password. If password is set,
            all communication with Delicious.com is SSL-encrypted.
        @type password: unicode/str

        @param max_bookmarks: Optional, default: 50.
            See the documentation of get_bookmarks() for more
            information as get_url() uses get_bookmarks() to
            retrieve a url's bookmarking history.
            The parameter is NOT used when a password is specified
            because in this case the *full* bookmark collection of
            a user will be retrieved.
        @type max_bookmarks: int

        @param sleep_seconds: Optional, default: 1.
            See the documentation of get_bookmarks() for more information as
            get_url() uses get_bookmarks() to retrieve a url's bookmarking
            history. sleep_seconds must be >= 1 to comply with Delicious.com's
            Terms of Use.
        @type sleep_seconds: int

        @return: DeliciousUser instance

        """
        assert username
        user = DeliciousUser(username)
        bookmarks = []
        if password:
            # We have username AND password, so we call
            # the official Delicious.com API.
            path = "/v1/posts/all"
            data = self._query(path, host="api.del.icio.us", use_ssl=True, user=username, password=password)
            if data:
                elements = etree.fromstring(data)
                for element in elements:
                    url = element.get("href")
                    title = element.get("description")
                    comment = element.get("extended")
                    tags_content = element.get("tag")
                    if tags_content:
                        tags = tags_content.split()
                    else:
                        tags = []
                    bookmarks.append((url,tags,title,comment))

            user.bookmarks = bookmarks
        else:
            # We have only the username, so we extract data from
            # the user's JSON feed. However, the feed is restricted
            # to the most recent public bookmarks of the user, which
            # is about 100 if any. So if we need more than 100, we start
            # scraping the Delicious.com website directly
            if max_bookmarks > 0 and max_bookmarks <= 100:
                path = "/v2/json/%s?count=100" % username
                data = self._query(path, host="feeds.delicious.com", user=username)
                if data:
                    posts = []
                    try:
                        posts = simplejson.loads(data)
                    except TypeError:
                        pass

                    url = None
                    title = comment = u""
                    tags = []

                    for post in posts:
                        # url
                        try:
                            url = post['u']
                        except KeyError:
                            pass
                        # title
                        try:
                            title = post['d']
                        except KeyError:
                            pass
                        # tags
                        try:
                            tags = post['t'] or []
                        except KeyError:
                            pass
            
                        # comment / notes
                        try:
                            comment = post['n']
                        except KeyError:
                            pass
                        # bookmark creation time
                       
                        bookmarks.append( (url, tags, title, comment) )
                    user.bookmarks = bookmarks[:max_bookmarks]
            else:
                # TODO: retrieve the first 100 bookmarks via JSON before
                #       falling back to scraping the delicous.com website
                user.bookmarks = self.get_bookmarks(username=username, max_bookmarks=max_bookmarks, sleep_seconds=sleep_seconds)
        return user

    def get_urls(self, tag=None, max_urls=100, sleep_seconds=1):
        """
        Returns the list of recent URLs (of web documents) tagged with a given tag.

        This is very similar to parsing Delicious' RSS/JSON feeds directly,
        but this function will return up to 2,000 links compared to a maximum
        of 100 links when using the official feeds (with query parameter
        count=100).

        The return list of links will be sorted by recency in descending order,
        i.e. newest items first.

        Note that even when setting max_urls, get_urls() cannot guarantee that
        it can retrieve *at least* this many URLs. It is really just an upper
        bound.

        @param tag: Retrieve links which have been tagged with the given tag.
            If tag is not set (default), links will be retrieved from the
            Delicious.com front page (aka "delicious hotlist").
        @type tag: unicode/str

        @param max_urls: return the maximum number urls,0 will return all the 
        urls by the tag given,if max_urls set,it will return maximum number urls,
        default is 100.
        @type max_urls: int

        @param sleep_seconds: Optional, default: 1.
            Wait the specified number of seconds between subsequent queries in
            case that there are multiple pages of bookmarks for the given url.
            Must be greater than or equal to 1 to comply with Delicious.com's
            Terms of Use.
            See also parameter 'max_urls'.
        @type sleep_seconds: int

        @return: The list of recent URLs (of web documents) tagged with a given tag.

        """
        assert sleep_seconds >= 1
        urls = []
        path = None
        if tag is None or (tag is not None and max_urls > 0 and max_urls <= 100):
            # use official JSON feeds
            max_json_count = 100
            if tag:                # tag-specific JSON feed
                path = "/v2/json/%s?count=%d" % (tag, max_json_count)
            else:
                # Delicious.com hotlist
                path = "/v2/json/?count=%d" % (max_json_count)
            data = self._query(path, host="feeds.delicious.com")
            if data:
                posts = []
                try:
                    posts = simplejson.loads(data)
                except TypeError:
                    pass

                for post in posts:
                    # url
                    try:
                        url = post['u']
                        if url:
                            urls.append(url)
                    except KeyError:
                        pass
        else:
            # maximum number of urls/posts Delicious.com will display
            # per page on its website
            max_html_count = 20
            max_html_pages = max_urls/max_html_count

          
            path = "/tag/%s" %tag 
            page_index = 1
            urls = []
            while path and page_index <= max_html_pages+1:
                data = self._query(path)
                path = None
                if data:
                    # extract urls from current page
                    tree = etree.HTML(data)
                    print "parsing %s page" %page_index
                    link_lists = tree.xpath("//div[@class='link  None']",smart_strings=False)
                    for link in link_lists:
                        url = link.xpath(".//a[@class='title']/@href",smart_strings=False)[0].split("url=")[-1]
                        urls.append(url)
                    print 'length of urls is ', len(urls)

                    paginations = tree.xpath("//div[@id='pagination']",smart_strings=False)

                    if paginations:
                        # find next path
                        nexts = tree.xpath("//div[@id='pagination']/span/following::a[1]/@href",smart_strings=False)
                        if nexts and (max_urls == 0 or len(urls) < max_urls) and len(urls) > 0:
                            path = '/tag/%s' %tag + nexts[0]
                            print 'path is', path
                            page_index += 1
                            time.sleep(sleep_seconds)
        if max_urls > 0:
            return urls[:max_urls]
        else:
            return urls
            


    def get_tags_of_user(self, username):
        """
        Retrieves user's public tags and their tag counts from Delicious.com.
        The tags represent a user's full public tagging vocabulary.

        DeliciousAPI uses the official JSON feed of the user. We could use
        RSS here, but the JSON feed has proven to be faster in practice.

        @param username: The Delicious.com username.
        @type username: str

        @return: Dictionary mapping tags to their tag counts.

        """
        tags = {}
        path = "/v2/json/tags/%s" % username
        data = self._query(path, host="feeds.delicious.com")
        if data:
            try:
                tags = simplejson.loads(data)
                if isinstance(tags,dict):
                    return tags
                else:
                    raise Exception("no tags for the user")
            except TypeError:
                pass
        return tags

    def get_number_of_users(self, url):
        """get_number_of_users() is obsolete and has been removed. Please use get_url() instead."""
        reason = "get_number_of_users() is obsolete and has been removed. Please use get_url() instead."
        raise Exception(reason)

    def get_common_tags_of_url(self, url):
        """get_common_tags_of_url() is obsolete and has been removed. Please use get_url() instead."""
        reason = "get_common_tags_of_url() is obsolete and has been removed. Please use get_url() instead."
        raise Exception(reason)

    def _html_escape(self, s):
        """HTML-escape a string or object.

        This converts any non-string objects passed into it to strings
        (actually, using unicode()).  All values returned are
        non-unicode strings (using "&#num;" entities for all non-ASCII
        characters).

        None is treated specially, and returns the empty string.

        @param s: The string that needs to be escaped.
        @type s: str

        @return: The escaped string.

        """
        if s is None:
            return ''
        if not isinstance(s, basestring):
            if hasattr(s, '__unicode__'):
                s = unicode(s)
            else:
                s = str(s)
        s = cgi.escape(s, True)
        if isinstance(s, unicode):
            s = s.encode('ascii', 'xmlcharrefreplace')
        return s


class DeliciousError(Exception):
    """Used to indicate that an error occurred when trying to access Delicious.com via its API."""

class DeliciousWarning(Exception):
    """Used to indicate a warning when trying to access Delicious.com via its API.

    Warnings are raised when it is useful to alert the user of some condition
    where that condition doesn't warrant raising an exception and terminating
    the program. For example, we issue a warning when Delicious.com returns a
    HTTP status code for redirections (3xx).
    """

class DeliciousThrottleError(DeliciousError):
    """Used to indicate that the client computer (i.e. its IP address) has been temporarily blocked by Delicious.com."""
    pass

class DeliciousUnknownError(DeliciousError):
    """Used to indicate that Delicious.com returned an (HTTP) error which we don't know how to handle yet."""
    pass

class DeliciousUnauthorizedError(DeliciousError):
    """Used to indicate that Delicious.com returned a 401 Unauthorized error.

    Most of the time, the user credentials for accessing restricted functions
    of the official Delicious.com API are incorrect.

    """
    pass

class DeliciousForbiddenError(DeliciousError):
    """Used to indicate that Delicious.com returned a 403 Forbidden error.
    """
    pass


class DeliciousNotFoundError(DeliciousError):
    """Used to indicate that Delicious.com returned a 404 Not Found error.

    Most of the time, retrying some seconds later fixes the problem
    (because we only query existing pages with this API).

    """
    pass

class Delicious500Error(DeliciousError):
    """Used to indicate that Delicious.com returned a 500 error.

    Most of the time, retrying some seconds later fixes the problem.

    """
    pass

class DeliciousMovedPermanentlyWarning(DeliciousWarning):
    """Used to indicate that Delicious.com returned a 301 Found (Moved Permanently) redirection."""
    pass

class DeliciousMovedTemporarilyWarning(DeliciousWarning):
    """Used to indicate that Delicious.com returned a 302 Found (Moved Temporarily) redirection."""
    pass

__all__ = ['DeliciousAPI', 'DeliciousURL', 'DeliciousError', 'DeliciousThrottleError', 'DeliciousUnauthorizedError', 'DeliciousUnknownError', 'DeliciousNotFoundError' , 'Delicious500Error', 'DeliciousMovedTemporarilyWarning']

if __name__ == "__main__":
    d = DeliciousAPI()
    print "Retrieving Delicious.com information about tag"
    print "Note: This might take some time..."
    print "========================================================="
    p =  d.get_urls('video',max_urls=101)
    print p
    print len(p)
