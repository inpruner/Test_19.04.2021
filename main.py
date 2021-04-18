from sqlalchemy import create_engine
from sqlalchemy import Column
from sqlalchemy import Integer, Text
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker


class Unit:
    all_units = {}

    def __init__(self, name=None):
        self.name = name
        self.__load_max = 0
        self.streams_in = dict()
        self.streams_out = dict()
        Unit.all_units[self.name] = self

    def set_load_max(self, value=0):
        self.__load_max = value

    def __repr__(self):
        return f"<{self.__class__.__name__}(name={self.name}," \
               f" __load_max={self.__load_max}," \
               f" streams_in={self.streams_in}," \
               f" streams_out={self.streams_out})>"


class AVTUnit(Unit):
    pass


class RerunningUnit(Unit):
    pass


class Stream:
    all_streams = {}

    def __init__(self, name=None):
        self.name = name
        self.dep_units = []
        self.dst_units = []
        Stream.all_streams[self.name] = self

    def __repr__(self):
        return f"<Stream(name={self.name}," \
               f" dep_units={self.dep_units}," \
               f" dst_units={self.dst_units})>"


__Base = declarative_base()


class __UnitTable(__Base):
    __tablename__ = 'unit'
    id = Column(Integer, primary_key=True, nullable=False)
    name = Column(Text)
    type = Column(Integer)

    def __repr__(self):
        return f"<Unit(id={self.id}, name={self.name}, type={self.type})>"


class __StreamTable(__Base):
    __tablename__ = 'stream'
    id = Column(Integer, primary_key=True, nullable=False)
    name = Column(Text)

    def __repr__(self):
        return f"<Stream(id={self.id}, name={self.name})>"


class __UnitMaterialTable(__Base):
    __tablename__ = 'unit_material'
    unit_id = Column(Integer, primary_key=True)
    stream_id = Column(Integer, primary_key=True)
    feed_flag = Column(Integer)

    def __repr__(self):
        return f"<UnitMaterial(unit_id={self.unit_id}," \
               f" stream_id={self.stream_id}, feed_flag={self.feed_flag})>"


class __LoadMaxTable(__Base):
    __tablename__ = 'load_max'
    unit_id = Column(Integer, primary_key=True)
    value = Column(Integer)

    def __repr__(self):
        return f"<LoadMax(unit_id={self.unit_id}, value={self.value})>"


def get_units(session, verbose=False):
    query = session.query(__UnitTable.name, __UnitTable.type)
    for instance in query:
        if instance.type == 0:
            new_unit = AVTUnit(instance.name)
        elif instance.type == 1:
            new_unit = RerunningUnit(instance.name)
        if verbose:
            print(new_unit)


def set_load_max(session, verbose=False):
    query = session.query(__UnitTable.name, __LoadMaxTable.value)
    query = query.join(__LoadMaxTable,
                       __UnitTable.id == __LoadMaxTable.unit_id)
    for unit in Unit.all_units.values():
        load_max_value = query.filter(__UnitTable.name == unit.name).\
                                      first().value
        unit.set_load_max(load_max_value)
        if verbose:
            print(unit)


def main(dialect='sqlite:///', path='db.db'):
    engine = create_engine(''.join([dialect, path]), echo=False)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        get_units(session, verbose=True)
        #set_load_max(session, verbose=True)


if __name__ == '__main__':
    main()
