import operator
import datetime as pydt
from functools import wraps
import pandas.tseries.tools as tools
from pandas import Series
import pandas.lib as lib
import pandas as pd

# TODO how to manage dependencies? Better for caching (overridden at
# ConfigSource subclass level)
# TODO how to cache more efficiently?
# TODO test performance
# TODO how to link code version with config version with data version?
# TODO how to manage the diffs between versions?
# TODO SQL config source
# TODO git config source
# TODO convert todo list into github issues :p
# TODO regex item search
# TODO GUI explorer (part of overall data explorer)
# TODO serialization
# TODO better terminology for item name vs base
# TODO repr
# TODO store all info in returned dict

# NOTE to maintain large amount of data, subclass should maintain out of memory
# key-value store of items and a mapping of group/base/dates to item key, and
# possibly maintain the mapping out of memory as well. Maybe change code so the
# item info formatting happens on the fly by group

# NOTE group can designate table in SQL version.
# NOTE when using hierarchical key-value store, can subclass ConfigVersion and
# just expose the get method from underlying version store to conserve memory

def cached_property(f):
    @wraps(f)
    def g(self, *args, **kwargs):
        pname = '_' + str(f.__name__)
        if not hasattr(self, pname):
            setattr(self, pname, f(self, *args, **kwargs))
            if not hasattr(self, '_cached_properties'):
                self._cached_properties = set()
            self._cached_properties.add(pname)
            if not hasattr(self, '_clear_cached_prop'):
                self._clear_cached_prop = lambda: [delattr(self, p) for p in
                                                   list(self._cached_properties)]
        return getattr(self, pname)
    return property(fget=g)

class ConfigItem(object):

    ITEM = 'item'
    BASE = 'item_base'
    GROUP = 'group'
    START = 'start_date'
    END = 'end_date'

    def __init__(self, base, name, group=None, start_date=None, end_date=None,
                 **kwargs):
        self.base = base
        self.name = name
        self.group = group
        self.start_date = start_date
        self.end_date = end_date
        self._param_set = kwargs

    def __getattr__(self, name):
        return self._param_set[name]

class ConfigManager(object):

    def __init__(self, env_name, source, cache_class=dict):
        self.environment = env_name
        self.source = source
        self._cache = cache_class()
        self._cache_class = cache_class

    def get_config(self, name, group=None, date=None, version=None):
        """
        Get the configurations for the given item
        on the given date and version

        Parameters
        ----------
        name : str
            Data item name
        group : object, default None
            Data item group, if any
        date : str or datetime, default None
            Configuration date for the item. Use latest date if None
        version : int, str, datetime, default None
            Version hash or datetime

        Returns
        -------
        conf : dict
        """
        if date is None:
            date = self.latest_date(name)
        date = tools.to_datetime(date)

        if version is None:
            version = date

        vobj = self.get_version(version)
        rs = vobj.get_item(name, group, date)
        if rs is None:
            raise ValueError('Parameters were not found for %s (version %s) for %s'                             % (name, version, date))
        return rs

    def get_version(self, version):
        """
        Get all the items for a single version

        Parameters
        ----------
        version: object
            The version date, tag, or version id

        Returns
        -------
        ver: ConfigVersion
        """
        _, version = self.source.clean_version(version)
        vobj = self._cache.get(version, None)
        if vobj is None:
            vobj = self.source.get_version(version)
            self._cache[version] = vobj
        return vobj

    def __getitem__(self, version):
        return self.get_version(version)

class ConfigVersion(object):
    """
    Represents a single version
    """

    def __init__(self, version, tag=None, item_info=None, items=None,
                 closed_start=True, closed_end=False):
        self.version = version
        self.tag = tag
        self._closed_start = closed_start
        self._closed_end = closed_end
        self._raw_info = item_info
        self._item_store = items

    @cached_property
    def _item_info(self):
        if not hasattr(self, '_raw_info'):
            return None
        item_info = {}
        mapper = [ConfigItem.GROUP, ConfigItem.BASE]
        for (grp, base), base_dm in self._raw_info.groupby(mapper):
            base_dm.index.name = ConfigItem.ITEM
            base_dm = base_dm.reset_index().set_index([ConfigItem.START])
            if (len(base_dm) == 1 and base_dm.index[0] is None and
                base_dm.ix[0, ConfigItem.END] is None):
                obj = base_dm.ix[0, ConfigItem.ITEM]
            else:
                obj = base_dm.sort_index()
            item_info.setdefault(grp, {})[base] = obj
        return item_info

    def __getitem__(self, item_name):
        info = self._raw_info.xs(item_name)
        base = info[ConfigItem.BASE]
        group = info[ConfigItem.GROUP]
        start = info[ConfigItem.START]
        end = info[ConfigItem.END]

        return ConfigItem(base, item_name, group, start, end,
                          **self._item_store.get(item_name))

    def get_item(self, base, group=None, date=None):
        """
        Given the item base and optionally a group context and date, retrieve
        the set of key-value pairs for the right configuration item
        """
        group_set = self._item_info.get(group, None)
        if group_set is None:
            raise ValueError('Group %s group was not found' % group)
        base_set = group_set.get(base, None)
        if base_set is None:
            raise ValueError('No item base %s' % base)

        start, end = None, None

        try:
            if date is None:
                date = base_set.index[-1]

            start = base_set.index.asof(date)
            row = base_set.ix[start]
            op = operator.lt
            if self._closed_end:
                op = operator.le
            end = row[ConfigItem.END]
            if op(end, date):
                raise ValueError('No item found for %s on %s' % (base,
                                  date))
            item_name = row[ConfigItem.ITEM]
        except AttributeError:
            item_name = base_set

        return ConfigItem(base, item_name, group, start, end,
                          **self._item_store.get(item_name))

class ConfigSource(object):
    """
    Abstract class representing a source for configuration items. A ConfigSource
    stores configuration items linked to sets of key-value parameter pairs, some of
    which are 'dated' and others not. Each version contains a set of items and is
    designated by a version id and optionally a version tag. A dated item is an
    item that is only valid within a specified date range.
    """
    VER = 'version'
    TAG = 'tag'
    DATE = 'date'
    AUTHOR = 'author'

    def __init__(self, name, version_store=ConfigVersion, **kwargs):
        """
        Parameters
        ----------
        name : str
            Name of this configuration source
        """
        self.name = name
        self.version_store = version_store
        self._kwargs = kwargs

    def __getattr__(self, key):
        return self._kwargs[key]

    @cached_property
    def all_versions(self):
        """
        Creates a DataFrame where the index is the version hash and the columns
        are the tag (if any), author, and creation date

        Returns
        -------
        ver : DataFrame
        """
        return self._load_versions()

    @cached_property
    def _tag_to_ver(self):
        """ tag name -> version id """
        mapping = self._ver_to_tag
        return Series(mapping.index, mapping.values)

    @cached_property
    def _date_to_ver(self):
        """ date -> version id """
        mapping = self.all_versions.reset_index().set_index('date')
        return mapping[ConfigSource.VER].order()

    @property
    def _ver_to_tag(self):
        """version id -> tag"""
        return self.all_versions[ConfigSource.TAG].dropna()

    def clean_version(self, version):
        """
        Interpret the given version and return the canonical tag and version.
        If the version is a date or datetime, then look up the latest version
        that was created on or after the given date. Otherwise, look to see if
        it is a valid tag name first.

        Parameters
        ----------
        version : object
        """
        if isinstance(version, (pydt.date, pydt.datetime)):
            version = self.version_asof(version)

        if version in self._tag_to_ver:
            tag, version = version, self._tag_to_ver.get(version)
        else:
            if version not in self.all_versions.index:
                raise KeyError('Version %s not found')
            tag = self._ver_to_tag.get(version, None)

        return tag, version

    @property
    def latest_version(self):
        """ convenience to get the latest version objection """
        dtv = self._date_to_ver
        return dtv.values[-1]

    @property
    def latest_date(self):
        """ convenience to get the latest version date """
        dtv = self._date_to_ver
        return dtv.index[-1]

    def version_asof(self, date):
        """ get the latest version created on or after given date"""
        rs = self._date_to_ver.asof(lib.Timestamp(date))
        if pd.isnull(rs):
            raise ValueError('Version not found for %s' % date)
        return rs

    def __getitem__(self, version):
        return self.get_version(version)

    def get_version(self, version):
        """
        Retrieve the configurations in the given version

        Parameters
        ----------
        version : object

        Returns
        -------
        ver : ConfigVersion
        """
        tag, version = self.clean_version(version)
        item_info = self._load_item_info(version)
        items = self._load_items(version)
        return self.version_store(version, tag, item_info, items)

    def _load_item_info(self, version):
        """
        Item id is unique
        item id mapped to base, group, start_date, end_date
        Returns
        -------
        info : DataFrame
        """
        raise NotImplemented()

    def _load_items(self, version):
        """
        Item name to dict of key-value parameter sets
        """
        raise NotImplemented

    def _load_versions(self):
        """ Return all the versions  """
        raise NotImplemented()


