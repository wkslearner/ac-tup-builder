from abc import abstractmethod
from ac_tup_builder.context import DataContext
from ac_tup_builder.context import TupRecordService, LruTupRecordService


class TagsBuilder(object):
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def build_tags(self, dataContext:DataContext, tupRecordService:TupRecordService):
        pass


class DataSource(object):
    @abstractmethod
    def __init__(self):
        pass

    def next(self) -> DataContext:
        pass

    def close(self):
        pass


class CompositeTagsBuilder(TagsBuilder):
    tagsBuilders = None

    def __init__(self, tagsBuilders:list):
        self.tagsBuilders = tagsBuilders

    def build_tags(self, dataContext:DataContext, tupRecordService:TupRecordService):
        for tagsBuilder in self.tagsBuilders:
            tagsBuilder.build_tags(dataContext, tupRecordService)


class TupBuilder(object):
    def __init__(self, dataSource:DataSource, tagsBuilder: TagsBuilder, tupRecordService: TupRecordService=None):
        self.dataSource = dataSource
        self.tagsBuilder = tagsBuilder
        if tupRecordService is None:
            self.tupRecordService = LruTupRecordService(1000)
        else:
            self.tupRecordService = tupRecordService

    def build(self):
        try:
            while True:
                dataContext = self.dataSource.next()
                if dataContext is None:
                    break

                self.tagsBuilder.build_tags(dataContext, self.tupRecordService)

            self.tupRecordService.flush()
        finally:
            self.dataSource.close()