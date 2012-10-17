import unittest
import nose
import datetime as pydt
import numpy as np
import pandas as pd
from pandas import DataFrame, Index
from config import ConfigSource, ConfigVersion, ConfigItem, ConfigManager

class TestSource(ConfigSource):

    def _load_item_info(self, version):
        """
        Item name or id is unique
        item name/id mapped to base, group, start_date, end_date
        Returns
        -------
        info : DataFrame
        """
        g1 = 'quantmodel'
        b1 = 'us_largecap'
        i1 = g1 + '_' + b1
        s1 = pydt.datetime(2012, 1, 1)
        e1 = pydt.datetime(2012, 2, 1)
        s12 = pydt.datetime(2012, 2, 1)
        e12 = pydt.datetime(2012, 3, 1)

        g2 = 'riskmodel'
        b2 = 'US_RiskModel'
        i2 = g2 + '_' + b2

        idx = [i1 + '_' + s1.strftime('%Y%m%d'),
               i1 + '_' + s12.strftime('%Y%m%d'),
               i2]
        cols = [ConfigItem.GROUP, ConfigItem.BASE, ConfigItem.START,
                ConfigItem.END]
        lst = [[g1, b1, s1, e1],
               [g1, b1, s12, e12],
               [g2, b2, None, None]]
        return DataFrame(lst, idx, cols)

    def _load_items(self, version):
        """
        Item name to dict of key-value parameter sets
        """
        g1 = 'quantmodel'
        b1 = 'us_largecap'
        s1 = pydt.datetime(2012, 1, 1)
        i1 = g1 + '_' + b1 + '_' + s1.strftime('%Y%m%d')
        c1 = dict(universe='SPX', riskmodel='US_RiskModel', key=version)

        s12 = pydt.datetime(2012, 2, 1)
        i12 = g1 + '_' + b1 + '_' + s12.strftime('%Y%m%d')
        c12 = dict(universe='Russell', riskmodel='US_RiskModel', key=version+1)

        g2 = 'riskmodel'
        b2 = 'US_RiskModel'
        i2 = g2 + '_' + b2
        c2 = dict(a=1, b=2, c=5+version)

        return {i1 : c1, i12 : c12, i2 : c2}

    def _load_versions(self):
        """ Return all the versions  """
        versions = np.arange(10)
        tags = [np.nan] * 10
        tags[0] = '1.0'
        tags[4] = '2.0'
        dates = pd.date_range('2012-1-1', periods=len(versions), freq='W')
        author = ['Mr. Foo', 'Ms. Bar'] * 5
        cols = [ConfigSource.TAG, ConfigSource.DATE, ConfigSource.AUTHOR,
                ConfigSource.VER]
        rs = DataFrame(np.array([tags, dates, author, versions]).T, columns=cols)
        return rs.set_index(ConfigSource.VER)

class TestConfigItem(unittest.TestCase):

    def setUp(self):
        self.src = TestSource('test', x=5)
        self.ver = self.src['2.0']

    def test_getattr(self):
        item = self.ver['quantmodel_us_largecap_20120201']
        assert item.universe == 'Russell'
        assert item.riskmodel == 'US_RiskModel'
        assert item.key == 5

class TestConfigSource(unittest.TestCase):

    def setUp(self):
        self.src = TestSource('test', x=5)

    def test_constructor(self):
        assert self.src.name == 'test', 'name wrong'
        assert hasattr(self.src, 'x'), "property x doesn't exist"
        assert self.src.x == 5, 'property x not set right'

    def test_all_versions(self):
        all_ver = self.src.all_versions
        assert len(all_ver) == 10, 'Must have 10 versions'
        assert type(all_ver) == DataFrame, 'Must be a DataFrame'
        xp_cols = Index([ConfigSource.TAG, ConfigSource.DATE,
                         ConfigSource.AUTHOR])
        assert all_ver.columns.equals(xp_cols)
        assert all_ver.index.name == ConfigSource.VER

    def test_latest(self):
        assert self.src.latest_version == 9
        assert self.src.latest_date == pydt.datetime(2012, 3, 4)

    def test_clean_version(self):
        # date, datetime
        v = pydt.date(2012, 2, 5)
        tag, version = self.src.clean_version(v)
        assert tag is None
        assert version == 5

        v = pydt.datetime(2012, 1, 1)
        tag, version = self.src.clean_version(v)
        assert tag == '1.0'
        assert version == 0

        # tag name
        v = '2.0'
        tag, version = self.src.clean_version(v)
        assert tag == v
        assert version == 4

        # version
        v = 8
        tag, version = self.src.clean_version(v)
        assert tag is None
        assert version == v

        # invalid
        v = 10
        self.assertRaises(KeyError, self.src.clean_version, v)

    def test_version_asof(self):
        before = pydt.datetime(2011, 12, 31)
        ondate = pydt.datetime(2012, 1, 29)
        offdate = pydt.datetime(2012, 2, 15)
        after = pydt.datetime(2012, 12, 31)

        self.assertRaises(ValueError, self.src.version_asof, before)

        assert self.src.version_asof(ondate) == 4
        assert self.src.version_asof(offdate) == 6
        assert self.src.version_asof(after) == 9

    def test_get_version(self):
        vobj = self.src.get_version('1.0')
        assert isinstance(vobj, ConfigVersion)

        vobj2 = self.src['1.0']
        assert isinstance(vobj2, ConfigVersion)

        assert vobj.version == vobj2.version
        assert vobj.tag == '1.0'
        assert vobj2.tag == '1.0'


class TestConfigVersion(unittest.TestCase):


    def setUp(self):
        self.src = TestSource('test', x=5)
        self.ver = self.src['2.0']

    def test_constructor(self):
        self.ver.version == 4
        self.ver.tag == '2.0'

    def test_get_item(self):
        item = self.ver.get_item('us_largecap', 'quantmodel')
        item2 = self.ver.get_item('us_largecap', 'quantmodel',
                              pydt.datetime(2012, 2, 15))
        assert item.base == 'us_largecap'
        assert item.group == 'quantmodel'
        assert item.name == 'quantmodel_us_largecap_20120201'
        assert item.start_date == pydt.datetime(2012, 2, 1)
        assert item.end_date == pydt.datetime(2012, 3, 1)

        assert item.base == item2.base
        assert item.group == item2.group
        assert item.name == item2.name


        item3 = self.ver.get_item('US_RiskModel', 'riskmodel',
                                  pydt.datetime(2012, 2, 15))
        assert item3.base == 'US_RiskModel'
        assert item3.group == 'riskmodel'

        assert item3.c == 9

    def test_versions(self):

        ver1 = self.src[1]
        item = ver1.get_item('us_largecap', 'quantmodel')
        assert item.universe == 'Russell'
        assert item.key == 2

        ver2 = self.src[2]
        item = ver2.get_item('us_largecap', 'quantmodel',
                             pydt.datetime(2012, 1, 31))
        item2 = ver2[item.name]
        assert item.name == item2.name
        assert item.universe == 'SPX'
        assert item.key == 2

class TestConfigManager(unittest.TestCase):

    def setUp(self):
        self.src = TestSource('test', x=5)
        self.mgr = ConfigManager('prod', self.src)

    def test_get_version(self):
        vobj = self.mgr['2.0']
        assert isinstance(vobj, ConfigVersion)
        assert vobj.version == 4

        vobj2 = self.mgr.get_version(pydt.datetime(2012, 2, 1))
        assert vobj.tag == '2.0'
        assert vobj is vobj2

        vobj = self.mgr.get_version(2)
        assert vobj.tag is None
        assert vobj.version == 2

    def test_get_config(self):
        name = 'us_largecap'
        date = pydt.datetime(2012, 1, 31)
        vdate = pydt.datetime(2012, 1, 29)
        item = self.mgr.get_config(name, 'quantmodel', date) # 2012-1-31
        assert isinstance(item, ConfigItem)
        assert item.universe == 'SPX'
        assert item.key == 4

        item_v0 = self.mgr.get_config(name, 'quantmodel', date, '1.0')
        assert isinstance(item_v0, ConfigItem)
        assert item_v0.key == 0

        item_v8 = self.mgr.get_config(name, 'quantmodel', date, 8)
        assert isinstance(item_v8, ConfigItem)
        assert item_v8.key == 8

        item_v4 = self.mgr.get_config(name, 'quantmodel', date, vdate)
        assert isinstance(item_v4, ConfigItem)
        assert item_v4.key == 4

if __name__ == '__main__':
    nose.runmodule(argv=[__file__,'-vvs','-x','--pdb', '--pdb-failure'],
                   exit=False)
