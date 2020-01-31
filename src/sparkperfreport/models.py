from enum import Enum as Eenum

from collections import defaultdict

import statistics

from sqlalchemy import (
    Column,
    DECIMAL,
    Enum,
    ForeignKey,
    Integer,
    Unicode,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship, backref
from sqlalchemy.orm.collections import attribute_mapped_collection

from . import Base, DBSession

from .constants import MLLIB_TESTS, STATISTICS_PARAMS


class SparkPerfTestPackEnum(Eenum):
    mllib = 1
    decision_tree = 2

    @classmethod
    def to_enum(cls, value):
        if value == 'decision-tree':
            return cls.decision_tree
        elif value == 'mllib':
            return cls.mllib
        raise KeyError('Value {} not found'.format(value))


class SparkPerfTestResultTypeEnum(Eenum):
    training_time = 1
    test_time = 2

    @classmethod
    def names(cls):
        for i in SparkPerfTestResultTypeEnum.__members__.keys():
            yield i

    @classmethod
    def to_enum(cls, value):
        return SparkPerfTestResultTypeEnum.__members__[value]


class SparkPerfTestingResults:
    def __init__(self, data):
        self.training_metrics = []
        self.training_time = []
        self.test_metric = []
        self.test_time = []
        only_time = False
        for i in data:
            if only_time:
                self.test_time.append(i.get('time'))
                continue
            if i.get('trainingMetric'):
                self.training_metrics.append(i.get('trainingMetric'))
            if i.get('trainingTime'):
                self.training_time.append(i.get('trainingTime'))
            if i.get('testMetric'):
                self.test_metric.append(i.get('testMetric'))
            if i.get('testTime'):
                self.test_time.append(i.get('testTime'))
            if not self.test_time:
                only_time = True
                self.test_time.append(i.get('time'))


class SparkPerfTestPack(Base):
    __tablename__ = 'spark_perf_test_pack'
    __table_args__ = (
        UniqueConstraint(
            'cluster_id', 'name',
            name='spark_perf_test_pack_test_unique_uix'),
    )

    id = Column(Integer, primary_key=True)
    cluster_id = Column(
        Integer,
        ForeignKey(
            'spark_perf_cluster_test.id',
            onupdate='CASCADE',
            ondelete='CASCADE'
        ),
        nullable=False, index=True, info={'skip_filters': True}
    )

    name = Column(Enum(SparkPerfTestPackEnum), nullable=False)

    def process_data(self, data):
        keys = MLLIB_TESTS
        if self.name == SparkPerfTestPackEnum.decision_tree:
            keys = data[self.name.name].keys()
        for test_key in keys:
            test_data = data[self.name.name][test_key]
            for k, v in SparkPerfTestResultTypeEnum.__members__.items():
                test_result = SparkPerfTestResult(
                    test_pack=self,
                    result_type=v,
                    test_name=test_key
                )
                test_result.process_data(test_data[k])
                self.results.append(test_result)

    def to_dict(self):
        result = defaultdict(dict)
        for i in self.results:
            result[i.test_name][i.result_type.name] = i.to_dict()
        return result


class SparkPerfTestResult(Base):
    __tablename__ = 'spark_perf_test_result'

    __table_args__ = (
        UniqueConstraint(
            'test_pack_id', 'test_name', 'result_type',
            name='spark_perf_test_result_test_unique_uix'),
    )

    id = Column(Integer, primary_key=True)
    test_pack_id = Column(
        Integer,
        ForeignKey(
            'spark_perf_test_pack.id',
            onupdate='CASCADE',
            ondelete='CASCADE'
        ),
        nullable=False, index=True, info={'skip_filters': True}
    )

    test_name = Column(Unicode(100), nullable=False)
    result_type = Column(Enum(SparkPerfTestResultTypeEnum), nullable=False)
    stdev = Column(DECIMAL(10, 4), nullable=False)
    mean = Column(DECIMAL(10, 4), nullable=False)
    median = Column(DECIMAL(10, 4), nullable=False)

    test_pack = relationship(SparkPerfTestPack, backref=backref('results'))

    def process_data(self, data):
        for func in STATISTICS_PARAMS:
            try:
                val = getattr(statistics, func)(
                    data.get(self.result_type)
                )
            except (TypeError, ValueError):
                val = 0
            setattr(self, func, val)

    def to_dict(self):
        return {
            f: getattr(self, f)
            for f in STATISTICS_PARAMS
        }


class SparkPerfClusterTest(Base):
    __tablename__ = 'spark_perf_cluster_test'

    id = Column(Integer, primary_key=True)
    cluster_label = Column(Unicode(100), nullable=False, unique=True)

    test_packs_data = relationship(
        SparkPerfTestPack,
        collection_class=attribute_mapped_collection('name'),
    )
    test_packs = relationship(SparkPerfTestPack, backref=backref('cluster'))

    def to_dict(self):
        return {
            k.name: v.to_dict()
            for k, v in self.test_packs_data.items()
        }

    def process_data(self, data):
        # TODO: change for SparkPerfTestPackEnum
        for testpack in ['decision-tree', 'mllib']:
            try:
                test_name = SparkPerfTestPackEnum.to_enum(testpack)
            except KeyError:
                continue

            test_pack = SparkPerfTestPack(
                cluster=self,
                name=test_name
            )

            test_pack.process_data(data)
            DBSession.add(test_pack)
            DBSession.flush()
