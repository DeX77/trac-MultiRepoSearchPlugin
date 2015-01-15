import posixpath
import pysolr
from trac.config import Option, ConfigurationError
from trac.core import *
from trac.mimeview.api import Mimeview
from trac.versioncontrol.api import Node

from multireposearch.interfaces import IMultiRepoSearchBackend


CONFIG_SECTION_NAME = 'multireposearch'
CONFIG_FIELD = {
'solr_url': (
CONFIG_SECTION_NAME,
'solr_url',
 None,
),
'timeout': (
CONFIG_SECTION_NAME,
'timeout',
'30',
),
}


class SolrIndexer(Component):
    implements(IMultiRepoSearchBackend)

    solr_url = Option('multireposearch', 'solr_url', '',
                      doc="""Url to Solr Instance""")

    timeout = Option('multireposearch', 'timeout', '30',
                      doc="""Timeout for Solr Connection""")


    def __init__(self):
        solr_url = self.config.get(*CONFIG_FIELD['solr_url'])
        timeout = self.config.getfloat(*CONFIG_FIELD['timeout'])

        if not solr_url:
            raise ConfigurationError('PySolrSearchBackend must be configured in trac.ini')
        self.conn = pysolr.Solr(solr_url, timeout=timeout)


    ## internal methods
    def _last_known_rev(self, reponame):

        q = {
            "repo:%s", reponame
        }
        desc_ = {
            'fl': 'version',  # fields returned,
            'rows': 1,
            'sort': 'timestamp desc',

        }
        params = desc_
        indexed_rev = self.conn.search(q, **params)

        if indexed_rev.hits > 0 :
            return indexed_rev.docs[0]['version']
        else:
            return None

    def _walk_repo(self, repo, path):
        node = repo.get_node(path)
        basename = posixpath.basename(path)

        if node.kind == Node.DIRECTORY:
            for subnode in node.get_entries():
                for result in self._walk_repo(repo, subnode.path):
                    yield result
        else:
            yield node

    ## methods for IMultiRepoSearchBackend

    def reindex_repository(self, reponame, modified=None):
        repo = self.env.get_repository(reponame=reponame)
        youngest_rev = repo.youngest_rev


        last_known_rev = self._last_known_rev(reponame)
        if last_known_rev is not None and last_known_rev == youngest_rev:
            self.log.debug("Repo %s doesn't need reindexing" % reponame)
            return

        self.log.debug("Repo %s DOES need reindexing" % reponame)
        mimeview = Mimeview(self.env)

        if modified is None:
            iterator = self._walk_repo(repo, "/")
        else:
            iterator = (repo.get_node(path) for path in modified)

        for node in iterator:

            content = node.get_content()
            if content is None:
                continue
            content = mimeview.to_unicode(content.read(), node.get_content_type())

            self.log.debug("Adding file %s to repo %s with version %s" % (node.path, reponame, youngest_rev) )

            self.conn.add([
                {
                    "repo": reponame,
                    "filename": node.path,
                    "contents": content,
                    "version": youngest_rev,
                },
            ])
        self.conn.optimize()


    def find_words(self, query):
        results = self.conn(query)

        for result in results:
            yield result['filename'], result['repo']



