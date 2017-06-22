from abc import abstractmethod
import dpath.util
from ac_tup_builder.model import AbstractTupRecord, SqlTupRecordStorage
from lru import LRU
from ti_daf.sql_tx import session_scope


class DataContext(object):
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def get_value(self, path:str):
        pass

    @abstractmethod
    def set_value(self, path:str, value):
        pass


class DictDataContext(DataContext):
    data = None

    def __init__(self, data:dict):
        self.data = data

    def get_value(self, path:str):
        return dpath.util.get(self.data, path)

    def set_value(self, path:str, value):
        dpath.util.new(self.data, path, value)


class UpperCasePathDataContext(DictDataContext):
    def __init__(self, data: dict):
        super.__init__(data)

    def get_value(self, path:str):
        return dpath.util.get(self.data, path.upper())

    def set_value(self, path:str, value):
        dpath.util.new(self.data, path.upper(), value)


class SqlAlchemyRowDataContext(DataContext):
    def __init__(self, row):
        self.row = row
        self.data = dict()

    def get_value(self, path:str):
        try:
            return dpath.util.get(self.data, path)
        except KeyError:
            return self.row[path]

    def set_value(self, path:str, value):
        dpath.util.new(self.data, path, value)


class TupRecordService(object):
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def get_tup_record(self, tagPrefixName:str, gpartyId:str) -> AbstractTupRecord:
        pass

    @abstractmethod
    def flush(self):
        pass


class LruTupRecordService(TupRecordService):
    lruSize = 1000
    lru = None
    tupRecordStorage = SqlTupRecordStorage()

    def __init__(self, lruSize:int):
        self.lruSize = lruSize
        self.lru = LRU(self.lruSize, callback=self._lru_tup_record_evicted)

    def _lru_tup_record_evicted(self, gpartyId: str, tup_recs: dict):
        for tup_rec in tup_recs.values():
            self.tupRecordStorage.save(tup_rec)

    def get_tup_record(self, tagPrefixName:str, gpartyId:str) -> AbstractTupRecord:
        tup_recs = self.lru.get(gpartyId)
        if tup_recs is None:
            tup_recs = dict()
            self.lru[gpartyId] = tup_recs

        tup_rec = tup_recs.get(tagPrefixName)
        if tup_rec is None:
            tup_rec = self.tupRecordStorage.query_or_new(tagPrefixName, gpartyId)
            tup_recs[tagPrefixName] = tup_rec

        return tup_rec

    def flush(self):
        with session_scope() as session:
            for gpartyId, tup_recs in self.lru.items():
                for tup_rec in tup_recs.values():
                    self.tupRecordStorage.save(tup_rec)

        self.lru.clear()
